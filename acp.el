;;; acp.el --- ACP-compatible core over ACP Proxy -*- lexical-binding: t; -*-

;; Copyright (C) 2025

;; Author: emacs-acp-proxy contributors
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: tools, processes
;; URL: https://github.com/nicehiro/emacs-acp-proxy

;;; Commentary:
;;
;; acp.el provides an ACP (Agent Client Protocol) compatible API
;; matching acp.el, but routes all traffic through the Rust ACP proxy server.
;; This lets existing libraries that depend on acp.el keep working while using
;; the proxy for process management and streaming.
;;
;; Load this file before any library that does (require 'acp). It provides
;; both 'acp and 'acp features.

;;; Code:

(require 'cl-lib)
(require 'json)
(require 'jsonrpc)
(require 'map)
(require 'seq)
(require 'subr-x)

;; Optional traffic buffer integration.
(unless (fboundp 'acp-traffic-get-buffer)
  (defun acp-traffic-get-buffer (&key named)
    "Return or create a traffic buffer named NAMED."
    (get-buffer-create (or named "*acp traffic*"))))
(unless (fboundp 'acp-traffic-log-traffic)
  (defun acp-traffic-log-traffic (&key buffer direction kind message)
    "Best-effort traffic logger when acp-traffic is unavailable."
    (with-current-buffer (or buffer (get-buffer-create "*acp traffic*"))
      (goto-char (point-max))
      (insert (format "%s %s %s\n" direction kind (map-elt message :json))))))

(defconst acp-package-version "0.11.1")
(defconst acp--jsonrpc-version "2.0")
(defconst acp-proxy-handles-transcripts t
  "Non-nil when transcripts are written by the ACP proxy.")

(defvar acp-logging-enabled nil)
(defvar acp-instance-count 0)

;; ---------------------------------------------------------------------------
;; Global shared proxy state
;; ---------------------------------------------------------------------------

(defvar acp--shared-connection nil
  "Shared jsonrpc connection to the proxy process.")

(defvar acp--shared-process nil
  "Shared proxy process.")

(defvar acp--shared-clients 0
  "Number of live clients attached to the shared proxy.")

(defvar acp--shared-client-list nil
  "List of clients attached to the shared proxy.")

(defun acp--shared-register-client (client)
  "Register CLIENT with the shared proxy."
  (setq acp--shared-client-list (cons client (delq client acp--shared-client-list))))

(defun acp--shared-unregister-client (client)
  "Unregister CLIENT from the shared proxy."
  (setq acp--shared-client-list (delq client acp--shared-client-list)))

(defun acp--shared-target-clients (method params)
  "Return shared clients that should handle METHOD with PARAMS."
  (let* ((method-name (if (symbolp method) (symbol-name method) method))
         (session-id (acp--get params 'sessionId))
         (clients acp--shared-client-list))
    (cl-labels
        ((client-session-id (client)
           (or (map-elt client :session-id)
               (when-let ((buf (map-elt client :context-buffer))
                          ((buffer-live-p buf)))
                 (with-current-buffer buf
                   (when (and (boundp 'agent-shell--state)
                              (bound-and-true-p agent-shell--state))
                     (map-nested-elt agent-shell--state '(:session :id))))))))
    (cond
     ((and session-id
           (member method-name '("acp/sessionUpdate" "acp/permissionRequest")))
      (or (seq-filter (lambda (client)
                        (equal (client-session-id client) session-id))
                      clients)
          nil))
     (t clients)))))

(defun acp--shared-dispatch-notification (method params)
  "Dispatch a proxy notification to the appropriate shared client(s)."
  (let ((clients (acp--shared-target-clients method params)))
    (dolist (client (or clients acp--shared-client-list))
      (acp--jsonrpc-notification-dispatcher client method params))))

(defun acp--shared-dispatch-request (method params)
  "Dispatch a proxy request to a single appropriate shared client."
  (let* ((clients (acp--shared-target-clients method params))
         (client (or (car clients) (car acp--shared-client-list))))
    (when client
      (acp--jsonrpc-request-dispatcher client method params))))

;; ---------------------------------------------------------------------------
;; Customization
;; ---------------------------------------------------------------------------

(defcustom acp-proxy-program "emacs-acp-proxy"
  "Path to the ACP Proxy server binary."
  :type 'string
  :group 'acp)

(defcustom acp-log-level "info"
  "Log level for the ACP Proxy server."
  :type '(choice (const "trace")
          (const "debug")
          (const "info")
          (const "warn")
          (const "error"))
  :group 'acp)

(defcustom acp-log-file-directory temporary-file-directory
  "The directory for `lsp-proxy' server to generate log file."
  :type 'string
  :group 'acp)

;; Optional explicit proxy config file. When nil, the proxy uses its default.
(defcustom acp-proxy-config-file nil
  "Explicit config file path for the ACP proxy, or nil to use the default."
  :type '(choice (const :tag "Default config" nil)
          (file :tag "Config file path"))
  :group 'acp)

;; Timeout for long-running prompt requests (nil disables).
(defcustom acp-prompt-timeout nil
  "Timeout in seconds for prompt requests, or nil to disable."
  :type '(choice (const :tag "No timeout" nil)
          (number :tag "Seconds"))
  :group 'acp)

;; (defcustom acp-log-file nil
;;   "Path to the ACP Proxy log file. When nil, log to stderr."
;;   :type '(choice (const :tag "Stderr (default)" nil)
;;           (file :tag "Log file path"))
;;   :group 'acp)

(defcustom acp-temp-dir
  (expand-file-name "acp/" temporary-file-directory)
  "Directory for generated proxy config files."
  :type 'directory
  :group 'acp)

(defvar acp--log-file nil
  "Path to the current log file.")

(defun acp-open-log-file ()
  "Open the current ACP proxy log file."
  (interactive)
  (unless acp--log-file
    (user-error "acp--log-file is nil; logs are sent to stderr"))
  (unless (file-exists-p acp--log-file)
    (user-error "Log file does not exist: %s" acp--log-file))
  (find-file acp--log-file))

;; ---------------------------------------------------------------------------
;; Client construction
;; ---------------------------------------------------------------------------

(cl-defun acp-make-client (&key context-buffer command command-params environment-variables
                                request-sender notification-sender request-resolver
                                response-sender outgoing-request-decorator
                                proxy-program proxy-config-file agent-name)
  "Create an ACP client compatible with acp.el.

COMMAND and COMMAND-PARAMS are the ACP agent process to run via the proxy.
ENVIRONMENT-VARIABLES is a list of strings in the form \"VAR=foo\".
PROXY-PROGRAM overrides `acp-proxy-program'.
PROXY-CONFIG-FILE uses an existing proxy config file.
AGENT-NAME overrides the generated agent name.
Other arguments match acp.el semantics."
  (unless (or command agent-name)
    (error ":command (or :agent-name) is required"))
  (list (cons :context-buffer context-buffer)
        (cons :instance-count (acp--increment-instance-count))
        (cons :process nil)
        (cons :connection nil)
        (cons :command command)
        (cons :command-params command-params)
        (cons :environment-variables environment-variables)
        (cons :pending-requests ())
        (cons :pending-incoming-responses (make-hash-table :test 'equal))
        (cons :incoming-request-id 0)
        (cons :request-id 0)
        (cons :notification-handlers ())
        (cons :request-handlers ())
        (cons :error-handlers ())
        (cons :request-sender (or request-sender #'acp--request-sender))
        (cons :notification-sender (or notification-sender #'acp--notification-sender))
        (cons :request-resolver (or request-resolver #'acp--request-resolver))
        (cons :response-sender (or response-sender #'acp--response-sender))
        (cons :outgoing-request-decorator outgoing-request-decorator)
        (cons :proxy-program (or proxy-program acp-proxy-program))
        (cons :proxy-config-file (or proxy-config-file acp-proxy-config-file))
        (cons :agent-name agent-name)
        (cons :proxy-connected nil)
        (cons :connect-result nil)
        (cons :connect-state 'disconnected)
        (cons :connect-waiters ())
        (cons :pending-permission-requests (make-hash-table :test 'equal))))

(defun acp--client-started-p (client)
  "Return non-nil if CLIENT process has been started."
  (and (map-elt client :process)
       (process-live-p (map-elt client :process))
       (map-elt client :connection)
       (jsonrpc-running-p (map-elt client :connection))))

;; ---------------------------------------------------------------------------
;; Startup / shutdown
;; ---------------------------------------------------------------------------

(defun acp--env-alist (env-list)
  "Convert ENV-LIST of \"VAR=VAL\" strings to an alist."
  (let (out)
    (dolist (entry env-list (nreverse out))
      (when (string-match "\\`\\([^=]+\\)=\\(.*\\)\\'" entry)
        (push (cons (intern (match-string 1 entry)) (match-string 2 entry)) out)))))

(defun acp--agent-spec (client)
  "Return an agent spec plist for CLIENT."
  (list :command (map-elt client :command)
        :args (map-elt client :command-params)
        :env (acp--env-alist (map-elt client :environment-variables))))

(cl-defun acp--jsonrpc-request (client method params &optional callback error-callback (timeout :default))
  "Send JSON-RPC request to proxy for CLIENT." 
  (if (or callback error-callback)
      (apply #'jsonrpc-async-request
             (map-elt client :connection)
             (intern method)
             params
             :success-fn (if callback
                             (lambda (result)
                               (funcall callback (acp--normalize-object result)))
                           #'ignore)
             :error-fn (if error-callback
                           (lambda (err)
                             (funcall error-callback (acp--normalize-object err)))
                         #'ignore)
             (if (eq timeout :default)
                 '()
               (list :timeout timeout)))
    (acp--normalize-object
     (jsonrpc-request (map-elt client :connection) (intern method) params))))

(defun acp--connect-agent-params (client)
  "Build connect params for CLIENT."
  (let* ((agent-name (or (map-elt client :agent-name)
                         (error ":agent-name is required")))
         (command (map-elt client :command))
         (args (map-elt client :command-params))
         (env (acp--env-alist (map-elt client :environment-variables)))
         (params (append (list :agentName agent-name)
                         (when command
                           (append (list :command command
                                         :args (if args (vconcat args) []))
                                   (when (and env (not (null env)))
                                     (list :env env)))))))
    params))

(defun acp--finish-connect (client result)
  "Mark CLIENT as connected with RESULT."
  (map-put! client :proxy-connected t)
  (map-put! client :connect-result result)
  (map-put! client :connect-state 'connected))

(defun acp--fail-connect (client)
  "Mark CLIENT as disconnected after connect failure."
  (map-put! client :proxy-connected nil)
  (map-put! client :connect-state 'failed))

(cl-defun acp--drain-connect-waiters (client &key result error)
  "Drain and run connect waiters for CLIENT."
  (let ((waiters (reverse (map-elt client :connect-waiters))))
    (map-put! client :connect-waiters ())
    (dolist (waiter waiters)
      (let ((on-success (map-elt waiter :on-success))
            (on-failure (map-elt waiter :on-failure)))
        (if error
            (when on-failure
              (funcall on-failure error))
          (when on-success
            (funcall on-success result)))))))

(defun acp--connect-agent-sync (client)
  "Synchronously connect CLIENT to its agent."
  (let ((result (acp--jsonrpc-request
                 client
                 "acp/connectAgent"
                 (acp--connect-agent-params client))))
    (acp--finish-connect client result)
    result))

(cl-defun acp--connect-agent-async (&key client on-success on-failure)
  "Asynchronously connect CLIENT to its agent."
  (acp--jsonrpc-request
   client
   "acp/connectAgent"
   (acp--connect-agent-params client)
   (lambda (result)
     (acp--finish-connect client result)
     (acp--drain-connect-waiters client :result result)
     (when on-success
       (funcall on-success result)))
   (lambda (err)
     (acp--fail-connect client)
     (acp--drain-connect-waiters client :error err)
     (when on-failure
       (funcall on-failure err)))))

(cl-defun acp--ensure-connected (&key client on-success on-failure sync)
  "Ensure CLIENT is connected.

When SYNC is non-nil, connect synchronously and return the connect result.
When SYNC is nil, invoke ON-SUCCESS/ON-FAILURE asynchronously."
  (let ((state (or (map-elt client :connect-state)
                   (if (map-elt client :proxy-connected)
                       'connected
                     'disconnected))))
    (cond
     (sync
      (if (eq state 'connected)
          (map-elt client :connect-result)
        (condition-case err
            (acp--connect-agent-sync client)
          (error
           (acp--fail-connect client)
           (let ((err-obj `((code . -32603) (message . ,(error-message-string err)))))
             (when on-failure
               (funcall on-failure err-obj))
             (error "ACP connect failed: %s" err-obj))))))
     ((eq state 'connected)
      (when on-success
        (run-at-time 0 nil on-success (map-elt client :connect-result))))
     ((eq state 'connecting)
      (map-put! client :connect-waiters
                (cons (list (cons :on-success on-success)
                            (cons :on-failure on-failure))
                      (map-elt client :connect-waiters))))
     (t
      (map-put! client :connect-state 'connecting)
      (map-put! client :connect-waiters
                (cons (list (cons :on-success on-success)
                            (cons :on-failure on-failure))
                      (map-elt client :connect-waiters)))
      (acp--connect-agent-async :client client)))))

(cl-defun acp--start-client (&key client)
  "Start CLIENT." 
  (unless client
    (error ":client is required"))
  (unless (or (map-elt client :command) (map-elt client :agent-name))
    (error ":command (or :agent-name) is required"))
  (when (acp--client-started-p client)
    (error "Client already started"))
  (if (and acp--shared-connection
           acp--shared-process
           (process-live-p acp--shared-process)
           (jsonrpc-running-p acp--shared-connection))
      (progn
        (setq acp--shared-clients (1+ acp--shared-clients))
        (acp--shared-register-client client)
        (map-put! client :connection acp--shared-connection)
        (map-put! client :process acp--shared-process))
    (let* ((timestamp (format-time-string "%Y%m%d%H%M%S"))
           (random-num (random 100000))
           (filename (format "acp-%s-%05d.log" timestamp random-num))
           (proxy-program (map-elt client :proxy-program))
           (proxy-config (map-elt client :proxy-config-file))
           (stderr-buffer (get-buffer-create "*acp-stderr(shared)*"))
           (stderr-proc (make-pipe-process
                         :name "acp-stderr(shared)"
                         :buffer stderr-buffer
                         :filter (lambda (_process raw-output)
                                   (acp--log client "STDERR" "%s" (string-trim raw-output))
                                   (when-let ((std-error (cond
                                                          ((acp--parse-stderr-api-error raw-output)
                                                           (acp--parse-stderr-api-error raw-output))
                                                          ((not (string-empty-p (string-trim raw-output)))
                                                           `((code . -32603)
                                                             (message . ,raw-output))))))
                                     (dolist (handler (map-elt client :error-handlers))
                                       (funcall handler std-error)))))))
      (setq acp--log-file (concat acp-log-file-directory filename))
      (let* ((proxy-args (append (list "--stdio" "--log-level" acp-log-level)
                                 (when proxy-config
                                   (list "--config" proxy-config))
                                 (list "--log-file" acp--log-file)))
             (conn (jsonrpc-process-connection
                    :name "acp-shared"
                    :process
                    (lambda (_connection)
                      (make-process
                       :name "acp-shared"
                       :command (cons proxy-program proxy-args)
                       :connection-type 'pipe
                       :noquery t
                       :stderr stderr-proc))
                    :notification-dispatcher
                    (lambda (_conn method params)
                      (acp--shared-dispatch-notification method params))
                    :request-dispatcher
                    (lambda (_conn method params)
                      (acp--shared-dispatch-request method params))
                    :on-shutdown (lambda (connection)
                                   (acp--process-sentinel client connection)))))
        (setq acp--shared-connection conn)
        (setq acp--shared-process (jsonrpc--process conn))
        (setq acp--shared-clients 1)
        (setq acp--shared-client-list (list client))
        (map-put! client :connection acp--shared-connection)
        (map-put! client :process acp--shared-process)))))

(defun acp--process-sentinel (client connection)
  "Handle proxy process shutdown for CONNECTION." 
  (let* ((proc (jsonrpc--process connection))
         (event (cond
                 ((and proc (not (process-live-p proc)))
                  (format "exited (%s)" (process-status proc)))
                 (t "stopped"))))
    (acp--log client "PROCESS" "%s" event)
    (setq acp--shared-connection nil)
    (setq acp--shared-process nil)
    (setq acp--shared-clients 0)
    (setq acp--shared-client-list nil)
    (map-put! client :proxy-connected nil)
    (map-put! client :connect-state 'disconnected)
    (map-put! client :connect-waiters ())
    (map-put! client :connection nil)
    (map-put! client :process nil)))

(cl-defun acp-shutdown (&key client)
  "Shutdown ACP CLIENT and release resources." 
  (unless client
    (error ":client is required"))
  (when (acp--client-started-p client)
    (setq acp--shared-clients (max 0 (1- acp--shared-clients)))
    (acp--shared-unregister-client client)
    (when (and (<= acp--shared-clients 0)
               acp--shared-connection)
      (jsonrpc-shutdown acp--shared-connection))
    (map-put! client :proxy-connected nil)
    (map-put! client :connect-state 'disconnected)
    (map-put! client :connect-waiters ())
    (map-put! client :connection nil)
    (map-put! client :process nil))
  (when (buffer-live-p (acp-logs-buffer :client client))
    (kill-buffer (acp-logs-buffer :client client)))
  (when (buffer-live-p (acp-traffic-buffer :client client))
    (kill-buffer (acp-traffic-buffer :client client))))

;; ---------------------------------------------------------------------------
;; Subscription APIs (compatible with acp.el)
;; ---------------------------------------------------------------------------

(cl-defun acp-subscribe-to-notifications (&key client on-notification buffer)
  "Subscribe to incoming CLIENT notifications.

ON-NOTIFICATION is of the form: (lambda (notification))
and invoked with BUFFER as current." 
  (unless client
    (error ":client is required"))
  (unless on-notification
    (error ":on-notification is required"))
  (let ((handlers (map-elt client :notification-handlers)))
    (push (lambda (notification)
            (with-temp-buffer
              (with-current-buffer (or (when (buffer-live-p buffer)
                                         buffer)
                                       (when (buffer-live-p (map-elt client :context-buffer))
                                         (map-elt client :context-buffer))
                                       (current-buffer))
                (funcall on-notification notification))))
          handlers)
    (map-put! client :notification-handlers handlers)))

(cl-defun acp-subscribe-to-requests (&key client on-request buffer)
  "Subscribe to incoming CLIENT requests.

ON-REQUEST is of the form: (lambda (request))
and invoked with BUFFER as current." 
  (unless client
    (error ":client is required"))
  (unless on-request
    (error ":on-request is required"))
  (let ((handlers (map-elt client :request-handlers)))
    (push (lambda (request)
            (with-temp-buffer
              (with-current-buffer (or (when (buffer-live-p buffer)
                                         buffer)
                                       (when (buffer-live-p (map-elt client :context-buffer))
                                         (map-elt client :context-buffer))
                                       (current-buffer))
                (funcall on-request request))))
          handlers)
    (map-put! client :request-handlers handlers)))

(cl-defun acp-subscribe-to-errors (&key client on-error buffer)
  "Subscribe to agent errors using CLIENT.

ON-ERROR is of the form: (lambda (error))
and invoked with BUFFER as current." 
  (unless client
    (error ":client is required"))
  (unless on-error
    (error ":on-error is required"))
  (let ((handlers (map-elt client :error-handlers)))
    (push (lambda (error)
            (with-temp-buffer
              (with-current-buffer (or (when (buffer-live-p buffer)
                                         buffer)
                                       (when (buffer-live-p (map-elt client :context-buffer))
                                         (map-elt client :context-buffer))
                                       (current-buffer))
                (funcall on-error error))))
          handlers)
    (map-put! client :error-handlers handlers)))

;; ---------------------------------------------------------------------------
;; Outgoing requests/notifications (compatible with acp.el)
;; ---------------------------------------------------------------------------

(defun acp--get (obj key)
  "Get KEY from OBJ supporting plist/alist/hash with keyword or symbol keys." 
  (let* ((k1 key)
         (k2 (if (keywordp key) (intern (substring (symbol-name key) 1)) (intern (format ":%s" key)))))
    (cond
     ((hash-table-p obj) (or (gethash k1 obj) (gethash k2 obj)))
     ((and (listp obj) (keywordp (car obj))) (or (plist-get obj k1) (plist-get obj k2)))
     ((listp obj) (or (cdr (assoc k1 obj)) (cdr (assoc k2 obj))))
     (t nil))))

(defun acp--vector-of-chars-p (value)
  "Return non-nil if VALUE is a vector of character codes." 
  (and (vectorp value)
       (> (length value) 0)
       (seq-every-p #'integerp value)))

(defun acp--normalize-prompt (prompt)
  "Normalize ACP prompt value to proxy-compatible message." 
  (cond
   ((stringp prompt) prompt)
   ((acp--vector-of-chars-p prompt)
    (apply #'string (append prompt nil)))
   (t prompt)))

(defun acp--map-request (client request)
  "Return (METHOD PARAMS RESULT-MAPPER) for proxy request." 
  (let* ((method (map-elt request :method))
         (params (map-elt request :params)))
    (pcase method
      ("initialize"
       (let ((agent-name (or (map-elt client :agent-name)
                             (error ":agent-name is required"))))
         (list "acp/connectAgent"
               (list :agentName agent-name)
               (lambda (result) result))))
      ("authenticate"
       (let ((agent-name (or (map-elt client :agent-name)
                             (error ":agent-name is required")))
             (method-id (acp--get params 'methodId)))
         (list "acp/authenticate"
               (list :agentName agent-name
                     :authMethodId method-id)
               (lambda (result) result))))
      ("session/new"
       (let ((agent-name (or (map-elt client :agent-name)
                             (error ":agent-name is required")))
             (cwd (acp--get params 'cwd)))
         (list "acp/newSession"
               (list :agentName agent-name
                     :cwd cwd
                     :mcpServers (or (acp--get params 'mcpServers) [])
                     :_meta (acp--get params '_meta))
               (lambda (result) result))))
      ("session/prompt"
       (let ((session-id (acp--get params 'sessionId))
             (prompt (or (acp--get params 'prompt)
                         (acp--get params 'message))))
         (list "acp/prompt"
               (list :sessionId session-id
                     :message (acp--normalize-prompt prompt))
               (lambda (result) result))))
      ("session/set_mode"
       (list "acp/setMode"
             (list :sessionId (acp--get params 'sessionId)
                   :modeId (acp--get params 'modeId))
             (lambda (result) result)))
      ("session/set_model"
       (list "acp/setModel"
             (list :sessionId (acp--get params 'sessionId)
                   :modelId (acp--get params 'modelId))
             (lambda (result) result)))
      ("session/cancel"
       (list "acp/cancel"
             (list :sessionId (acp--get params 'sessionId)
                   :reason (acp--get params 'reason))
             (lambda (result) result)))
      ("session/list"
       (list "acp/listSessions" nil (lambda (result) result)))
      (_
       (list nil nil nil)))))

(cl-defun acp-send-request (&key client request buffer on-success on-failure sync)
  "Send REQUEST from CLIENT.

ON-SUCCESS is of the form (lambda (response)).
ON-FAILURE is of the form (lambda (error)).

When non-nil SYNC, send request synchronously.
When BUFFER is provided, callbacks executed within buffer context." 
  (unless client
    (error ":client is required"))
  (unless request
    (error ":request is required"))
  (unless (acp--client-started-p client)
    (acp--start-client :client client))
  (funcall (map-elt client :request-sender)
           :client client
           :request request
           :buffer buffer
           :on-success on-success
           :on-failure on-failure
           :sync sync))

(cl-defun acp-send-notification (&key client notification sync)
  "Send NOTIFICATION from CLIENT.

When non-nil SYNC, send notification synchronously." 
  (unless client
    (error ":client is required"))
  (unless notification
    (error ":notification is required"))
  (unless (acp--client-started-p client)
    (acp--start-client :client client))
  (funcall (map-elt client :notification-sender)
           :client client
           :notification notification
           :sync sync))

(cl-defun acp--request-sender (&key client request buffer on-success on-failure sync)
  "Send REQUEST from CLIENT via proxy." 
  (unless client
    (error ":client is required"))
  (unless request
    (error ":request is required"))
  (when-let ((decorator (map-elt client :outgoing-request-decorator)))
    (if-let ((decorated (funcall decorator request)))
        (setq request decorated)
      (acp--log client "DECORATOR ERROR"
                "Outgoing request decorator returned nil for \"%s\", sending original request"
                (map-elt request :method))))
  (pcase-let ((`(,proxy-method ,proxy-params ,result-mapper)
               (acp--map-request client request)))
    (unless proxy-method
      (let ((err (acp-make-error :code -32601 :message (format "Method not supported: %s" (map-elt request :method)))))
        (if on-failure
            (with-temp-buffer
              (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                (funcall on-failure err)))
          (error "ACP request failed: %s" err)))
      (cl-return-from acp--request-sender nil))
    (let ((timeout (if (string= proxy-method "acp/prompt")
                       acp-prompt-timeout
                     :default)))
      (if sync
          (progn
            (acp--ensure-connected :client client :on-failure on-failure :sync t)
            (condition-case err
                (let* ((result (if (string= proxy-method "acp/connectAgent")
                                   (map-elt client :connect-result)
                                 (acp--jsonrpc-request client proxy-method proxy-params nil nil timeout)))
                       (mapped (if result-mapper (funcall result-mapper result) result)))
                  mapped)
              (error
               (let ((err-obj `((code . -32603) (message . ,(error-message-string err)))))
                 (if on-failure
                     (with-temp-buffer
                       (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                         (funcall on-failure err-obj)))
                   (error "ACP request failed: %s" err-obj))))))
        (acp--ensure-connected
         :client client
         :on-success (lambda (_connect-result)
                       (if (string= proxy-method "acp/connectAgent")
                           (when on-success
                             (with-temp-buffer
                               (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                                 (funcall on-success
                                          (if result-mapper
                                              (funcall result-mapper (map-elt client :connect-result))
                                            (map-elt client :connect-result))))))
                         (acp--jsonrpc-request
                          client
                          proxy-method
                          proxy-params
                          (when on-success
                            (lambda (result)
                              (with-temp-buffer
                                (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                                  (funcall on-success
                                           (if result-mapper (funcall result-mapper result) result))))))
                          (when on-failure
                            (lambda (err)
                              (with-temp-buffer
                                (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                                  (funcall on-failure err)))))
                          timeout)))
         :on-failure (when on-failure
                       (lambda (err)
                         (with-temp-buffer
                           (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                             (funcall on-failure err)))))
         :sync nil)))))

(cl-defun acp--notification-sender (&key client notification sync)
  "Send NOTIFICATION from CLIENT via proxy." 
  (let* ((method (map-elt notification :method))
         (params (map-elt notification :params)))
    (pcase method
      ("session/cancel"
       (let ((cancel-params (list :sessionId (acp--get params 'sessionId)
                                  :reason (acp--get params 'reason))))
         (if sync
             (progn
               (acp--ensure-connected :client client :sync t)
               (acp--jsonrpc-request client "acp/cancel" cancel-params))
           (acp--ensure-connected
            :client client
            :on-success (lambda (_connect-result)
                          (acp--jsonrpc-request
                           client "acp/cancel" cancel-params
                           #'ignore #'ignore))
            :on-failure #'ignore
            :sync nil))))
      (_
       (acp--log client "NOTIFICATION" "Unsupported notification: %s" method)))))

(cl-defun acp-send-response (&key client response)
  "Send a request RESPONSE from CLIENT." 
  (unless client
    (error ":client is required"))
  (unless response
    (error ":response is required"))
  (let* ((request-id (map-elt response :request-id))
         (pending (and request-id
                       (gethash request-id (map-elt client :pending-incoming-responses)))))
    (if pending
        (progn
          (setcar pending t)
          (setcdr pending response))
      (funcall (map-elt client :response-sender)
               :client client
               :response response))))

(defun acp--extract-permission-outcome (response)
  "Extract (OUTCOME OPTION-ID) from RESPONSE." 
  (let* ((result (map-elt response :result))
         (outcome (or (acp--get result 'outcome) result))
         (outcome-type (or (acp--get outcome 'outcome)
                           (acp--get outcome 'status)))
         (option-id (acp--get outcome 'optionId)))
    (list outcome-type option-id)))

(defun acp--select-option-id (options)
  "Pick a best-effort option id from OPTIONS list." 
  (let ((opts (if (vectorp options) (append options nil) options))
        (fallback nil))
    (dolist (opt opts)
      (let* ((opt-id (or (acp--get opt 'id)
                         (acp--get opt 'optionId)))
             (label (or (acp--get opt 'label)
                        (acp--get opt 'title)
                        "")))
        (setq fallback (or fallback opt-id))
        (when (and opt-id (string-match-p "\`\(reject\|deny\|cancel\)\'" (downcase opt-id)))
          (cl-return-from acp--select-option-id opt-id))
        (when (and opt-id (string-match-p "reject\|deny\|cancel" (downcase label)))
          (cl-return-from acp--select-option-id opt-id))))
    fallback))

(cl-defun acp--response-sender (&key client response)
  "Send a request RESPONSE from CLIENT." 
  (let* ((request-id (map-elt response :request-id))
         (pending (and request-id
                       (gethash request-id (map-elt client :pending-permission-requests)))))
    (if (not pending)
        (acp--log client "RESPONSE" "Unhandled response id: %s" request-id)
      (let* ((outcome (acp--extract-permission-outcome response))
             (outcome-type (car outcome))
             (option-id (cadr outcome))
             (options (plist-get pending :options))
             (option-id (or option-id
                            (when (and outcome-type (string= outcome-type "cancelled"))
                              (acp--select-option-id options))
                            (acp--select-option-id options))))
        (remhash request-id (map-elt client :pending-permission-requests))
        (acp--jsonrpc-request
         client
         "acp/respondPermission"
         (list :requestId (plist-get pending :requestId)
               :sessionId (plist-get pending :sessionId)
               :optionId option-id)
         #'ignore #'ignore)))))

(defun acp--next-incoming-request-id (client)
  "Return a fresh ACP request id for incoming proxy requests." 
  (let ((next (1+ (or (map-elt client :incoming-request-id) 0))))
    (map-put! client :incoming-request-id next)
    (format "req-%s" next)))

(defun acp--await-incoming-response (client request-id waiter)
  "Wait for a RESPONSE to REQUEST-ID and return it." 
  (while (and (not (car waiter))
              (process-live-p (map-elt client :process)))
    (accept-process-output nil 0.05))
  (let ((response (cdr waiter)))
    (remhash request-id (map-elt client :pending-incoming-responses))
    response))

;; ---------------------------------------------------------------------------
;; Incoming proxy notifications -> ACP-compatible dispatch
;; ---------------------------------------------------------------------------

(defun acp--dispatch-notification (client method params)
  "Dispatch an ACP-compatible notification to CLIENT handlers." 
  (let ((notification `((jsonrpc . ,acp--jsonrpc-version)
                        (method . ,method)
                        (params . ,params))))
    (dolist (handler (map-elt client :notification-handlers))
      (condition-case-unless-debug err
          (funcall handler notification)
        (error (acp--log client "NOTIFICATION HANDLER ERROR" "%S" err))))))

(defun acp--dispatch-request (client method params request-id)
  "Dispatch an ACP-compatible request to CLIENT handlers." 
  (let ((request `((jsonrpc . ,acp--jsonrpc-version)
                   (method . ,method)
                   (id . ,request-id)
                   (params . ,params))))
    (dolist (handler (map-elt client :request-handlers))
      (condition-case-unless-debug err
          (funcall handler request)
        (error (acp--log client "REQUEST HANDLER ERROR" "%S" err))))))

(defun acp--jsonrpc-notification-dispatcher (client method params)
  "Handle incoming JSON-RPC notification METHOD with PARAMS." 
  (pcase (if (symbolp method) (symbol-name method) method)
    ("acp/sessionUpdate"
     (acp--dispatch-notification
      client "session/update" (acp--normalize-object params)))
    ("acp/permissionRequest"
     (let* ((normalized (acp--normalize-object params))
            (request-id (format "perm-%s" (map-elt client :request-id)))
            (session-id (acp--get normalized 'sessionId))
            (proxy-request-id (acp--get normalized 'requestId))
            (options (acp--get normalized 'options)))
       (map-put! client :request-id (1+ (map-elt client :request-id)))
       (puthash request-id
                (list :requestId proxy-request-id
                      :sessionId session-id
                      :options options)
                (map-elt client :pending-permission-requests))
       (acp--dispatch-request
        client
        "session/request_permission"
        normalized
        request-id)))
    ("acp/authRequired"
     (acp--dispatch-notification client "auth/required" (acp--normalize-object params)))
    ("acp/agentDisconnected"
     (acp--dispatch-notification client "agent/disconnected" (acp--normalize-object params)))
    ("acp/fileChanged"
     (acp--dispatch-notification client "fs/file_changed" (acp--normalize-object params)))
    (_
     (acp--dispatch-notification client
                                 (acp--normalize-method method)
                                 (acp--normalize-object params)))))

(defun acp--jsonrpc-request-dispatcher (client method params)
  "Handle incoming JSON-RPC request METHOD with PARAMS." 
  (let* ((method-name (acp--normalize-method method))
         (normalized (acp--normalize-object params))
         (request-id (acp--next-incoming-request-id client))
         (waiter (cons nil nil)))
    (puthash request-id waiter (map-elt client :pending-incoming-responses))
    (acp--dispatch-request client method-name normalized request-id)
    (let ((response (acp--await-incoming-response client request-id waiter)))
      (unless response
        (jsonrpc-error :code -32603
                       :message (format "No response for %s" method-name)))
      (if-let ((err (acp--get response 'error)))
          (jsonrpc-error :code (or (acp--get err 'code) -32603)
                         :message (or (acp--get err 'message) "Internal error")
                         :data (acp--get err 'data))
        (acp--get response 'result)))))

(defun acp--normalize-method (method)
  "Return METHOD as a string." 
  (cond
   ((symbolp method) (symbol-name method))
   ((stringp method) method)
   (t (format "%s" method))))

;; ---------------------------------------------------------------------------
;; ACP request/response constructors (copied for compatibility)
;; ---------------------------------------------------------------------------

(cl-defun acp-make-initialize-request (&key protocol-version
                                            client-info
                                            read-text-file-capability
                                            write-text-file-capability)
  "Instantiate an \"initialize\" request." 
  (unless protocol-version
    (error ":protocol-version is required"))
  `((:method . "initialize")
    (:params . (,@(when client-info
                    `((clientInfo . ,client-info)))
                (protocolVersion . ,protocol-version)
                (clientCapabilities . ((fs . ((readTextFile . ,(if read-text-file-capability t :false))
                                              (writeTextFile . ,(if write-text-file-capability t :false))))))))))

(cl-defun acp-make-authenticate-request (&key method-id method)
  "Instantiate an \"authenticate\" request." 
  (unless method-id
    (error ":method-id is required"))
  `((:method . "authenticate")
    (:params . ,(append `((methodId . ,method-id))
                        (when method
                          `((authMethod . ,method)))))))

(cl-defun acp-make-session-new-request (&key cwd mcp-servers meta)
  "Instantiate a \"session/new\" request." 
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/new")
    (:params . ((cwd . ,(directory-file-name (expand-file-name cwd)))
                (mcpServers . ,(or mcp-servers []))
                ,@(when meta `((_meta . ,meta)))))))

(cl-defun acp-make-session-prompt-request (&key session-id prompt)
  "Instantiate a \"session/prompt\" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless prompt
    (error ":prompt is required"))
  `((:method . "session/prompt")
    (:params . ((sessionId . ,session-id)
                (prompt . ,(vconcat prompt))))))

(cl-defun acp-make-session-set-mode-request (&key session-id mode-id)
  "Instantiate a \"session/set_mode\" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless mode-id
    (error ":mode-id is required"))
  `((:method . "session/set_mode")
    (:params . ((sessionId . ,session-id)
                (modeId . ,mode-id)))))

(cl-defun acp-make-session-set-model-request (&key session-id model-id)
  "Instantiate a \"session/set_model\" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless model-id
    (error ":model-id is required"))
  `((:method . "session/set_model")
    (:params . ((sessionId . ,session-id)
                (modelId . ,model-id)))))

(cl-defun acp-make-session-resume-request (&key session-id cwd mcp-servers)
  "Instantiate a \"session/resume\" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/resume")
    (:params . ((sessionId . ,session-id)
                (cwd . ,(directory-file-name (expand-file-name cwd)))
                (mcpServers . ,(or mcp-servers []))))))

(cl-defun acp-make-session-list-request (&key cwd)
  "Instantiate a \"session/list\" request." 
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/list")
    (:params . ((cwd . ,(directory-file-name (expand-file-name cwd)))))))

(cl-defun acp-make-session-load-request (&key session-id cwd mcp-servers)
  "Instantiate a \"session/load\" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/load")
    (:params . ((sessionId . ,session-id)
                (cwd . ,(directory-file-name (expand-file-name cwd)))
                (mcpServers . ,(or mcp-servers []))))))

(cl-defun acp-make-session-delete-request (&key session-id)
  "Instantiate a \"session/delete\" request." 
  (unless session-id
    (error ":session-id is required"))
  `((:method . "session/delete")
    (:params . ((sessionId . ,session-id)))))

(cl-defun acp-make-session-request-permission-response (&key request-id option-id cancelled)
  "Instantiate a \"session/request_permission\" response." 
  (unless request-id
    (error ":request-id is required"))
  (when (and option-id cancelled)
    (error "Choose :option-id or :cancelled Not both"))
  (unless (or option-id cancelled)
    (error "Must specify either :option-id or :cancelled"))
  `((:request-id . ,request-id)
    (:result . ((outcome . ,(if cancelled
                                '((outcome . "cancelled"))
                              `((outcome . "selected")
                                (optionId . ,option-id))))))))

(cl-defun acp-make-fs-read-text-file-response (&key request-id content error)
  "Instantiate a \"fs/read_text_file\" response." 
  (unless request-id
    (error ":request-id is required"))
  (cond
   ((and content error)
    (error "Either :content or :error but not both"))
   (error
    `((:request-id . ,request-id)
      (:error . ,error)))
   (content
    `((:request-id . ,request-id)
      (:result . ((content . ,content)))))
   (t
    (error "Either :content or :error is required"))))

(cl-defun acp-make-fs-write-text-file-response (&key request-id error)
  "Instantiate a \"fs/write_text_file\" response." 
  (unless request-id
    (error ":request-id is required"))
  (if error
      `((:request-id . ,request-id)
        (:error . ,error))
    `((:request-id . ,request-id)
      (:result . nil))))

(cl-defun acp-make-error (&key code message data)
  "Create a JSON-RPC error object." 
  (unless code
    (error ":code is required"))
  (unless message
    (error ":message is required"))
  (let ((error `((code . ,code)
                 (message . ,message))))
    (when data
      (nconc error `((data . ,data))))
    error))

(cl-defun acp-make-session-cancel-notification (&key session-id reason)
  "Instantiate a \"session/cancel\" request." 
  (unless session-id
    (error ":session-id is required"))
  `((:method . "session/cancel")
    (:params . ((sessionId . ,session-id)
                ,@(when reason `((reason . ,reason)))))))

;; ---------------------------------------------------------------------------
;; Logging (compatible with acp.el)
;; ---------------------------------------------------------------------------

(cl-defun acp--request-resolver (&key client id)
  "Resolve CLIENT request with ID to a handler." 
  (map-nested-elt client `(:pending-requests ,id)))

(cl-defun acp--make-message (&key json object)
  "Create message with JSON and OBJECT." 
  (list (cons :object object)
        (cons :json json)))

(cl-defun acp--parse-stderr-api-error (raw-output)
  "Parse RAW-OUTPUT, typically from stderr.

Returns non-nil if error was parseable." 
  (when (string-match "Attempt \\([0-9]+\\) failed with status \\([0-9]+\\)\\. Retrying.*ApiError: \\({.*}\\)" raw-output)
    (let ((error-json (match-string 3 raw-output)))
      (condition-case nil
          (let-alist (acp--parse-json error-json)
            (condition-case nil
                (map-elt (acp--parse-json .error.message) 'error)
              (error nil)))
        (error nil)))))

(defun acp--format-log-message (label format-string &rest args)
  "Return a log message formatted like `acp--log'." 
  (unless format-string
    (error ":format-string is required"))
  (let ((body (apply #'format format-string args)))
    (if label
        (format "%s >\n\n%s\n\n" label body)
      (format "%s\n\n" body))))

(defun acp--insert-log-entry (label format-string &rest args)
  "Insert a log message at point and add a boundary marker." 
  (let ((entry-start (point)))
    (insert (apply #'acp--format-log-message label format-string args))
    (when (< entry-start (point))
      (add-text-properties entry-start (1+ entry-start)
                           '(acp-log-boundary t)))))

(defun acp--log (client label format-string &rest args)
  "Log CLIENT message using LABEL, FORMAT-STRING, and ARGS." 
  (when acp-logging-enabled
    (let ((log-buffer (acp-logs-buffer :client client)))
      (with-current-buffer log-buffer
        (goto-char (point-max))
        (apply #'acp--insert-log-entry label format-string args))
      (acp--trim-log-buffer log-buffer))))

(defvar acp--log-buffer-max-bytes (* 100 1000 1000)
  "Maximum size of the log buffer in bytes.")

(defun acp--total-buffer-bytes (buffer)
  "Return the total number of bytes in BUFFER." 
  (with-current-buffer buffer
    (save-restriction
      (widen)
      (1- (position-bytes (point-max))))))

(defun acp--trim-log-buffer (buffer &optional max-bytes)
  "Trim BUFFER to a maximum size in bytes at log message boundaries." 
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (save-excursion
        (let ((max-bytes (or max-bytes acp--log-buffer-max-bytes))
              (total-bytes (acp--total-buffer-bytes (current-buffer))))
          (when (< max-bytes total-bytes)
            (goto-char (byte-to-position (- total-bytes max-bytes)))
            (when (get-text-property (point) 'acp-log-boundary)
              (forward-char 1))
            (delete-region (point-min)
                           (next-single-property-change
                            (point) 'acp-log-boundary nil (point-max)))))))))

(defun acp--json-pretty-print (json)
  "Return a pretty-printed JSON string." 
  (if acp-logging-enabled
      (with-temp-buffer
        (insert json)
        (json-pretty-print (point-min) (point-max))
        (buffer-string))
    json))

(defun acp--log-traffic (client direction kind message)
  "Log CLIENT traffic MESSAGE to "*acp traffic*" buffer." 
  (when acp-logging-enabled
    (acp-traffic-log-traffic
     :buffer (acp-traffic-buffer :client client)
     :direction direction :kind kind :message message)))

(defun acp--show-json-object (object)
  "Display OBJECT in a pretty-printed buffer." 
  (let ((json-buffer (get-buffer-create "*acp object*")))
    (with-current-buffer json-buffer
      (read-only-mode -1)
      (erase-buffer)
      (insert (json-encode object))
      (json-pretty-print-buffer)
      (goto-char (point-min))
      (read-only-mode 1))
    (display-buffer json-buffer)))

(cl-defun acp-reset-logs (&key client)
  "Reset CLIENT log buffers." 
  (with-current-buffer (acp-logs-buffer :client client)
    (erase-buffer))
  (with-current-buffer (acp-traffic-buffer :client client)
    (erase-buffer)))

(cl-defun acp-logs-buffer (&key client)
  "Get CLIENT logs buffer." 
  (if-let* ((name
             (format "*acp-(%s)-%s log*"
                     (or (map-elt client :command) "agent")
                     (map-elt client :instance-count)))
            (buffer (get-buffer name)))
      buffer
    (with-current-buffer (get-buffer-create name)
      (buffer-disable-undo)
      (current-buffer))))

(cl-defun acp-traffic-buffer (&key client)
  "Get CLIENT traffic buffer." 
  (acp-traffic-get-buffer :named (format "*acp-(%s)-%s traffic*"
                                         (or (map-elt client :command) "agent")
                                         (map-elt client :instance-count))))

(defun acp--increment-instance-count ()
  "Increment variable `acp-instance-count'." 
  (if (= acp-instance-count most-positive-fixnum)
      (setq acp-instance-count 0)
    (setq acp-instance-count (1+ acp-instance-count))))

(defun acp--parse-json (json)
  "Parse JSON using a consistent configuration." 
  (json-parse-string json :object-type 'alist :null-object nil :false-object nil))

(defun acp--serialize-json (object)
  "Serialize OBJECT to JSON using a consistent configuration." 
  (concat (json-serialize object) "\n"))

(defun acp--keyword-to-symbol (key)
  "Convert KEY keyword to symbol without leading colon." 
  (if (keywordp key)
      (intern (substring (symbol-name key) 1))
    key))

(defun acp--normalize-object (obj)
  "Normalize OBJ to use symbol keys in alists (not keywords).

Converts plists and hash tables into alists with symbol keys,
recursing into nested structures." 
  (cond
   ((hash-table-p obj)
    (let (out)
      (maphash (lambda (k v)
                 (push (cons (acp--keyword-to-symbol k)
                             (acp--normalize-object v))
                       out))
               obj)
      (nreverse out)))
   ((and (listp obj) (keywordp (car obj)))
    (let (out)
      (while obj
        (let ((k (car obj))
              (v (cadr obj)))
          (push (cons (acp--keyword-to-symbol k)
                      (acp--normalize-object v))
                out))
        (setq obj (cddr obj)))
      (nreverse out)))
   ((listp obj)
    (mapcar (lambda (el)
              (if (consp el)
                  (cons (acp--keyword-to-symbol (car el))
                        (acp--normalize-object (cdr el)))
                (acp--normalize-object el)))
            obj))
   ((vectorp obj)
    (apply #'vector (mapcar #'acp--normalize-object obj)))
   (t obj)))

(provide 'acp)

;;; acp.el ends here
