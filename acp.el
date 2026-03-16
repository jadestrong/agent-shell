;;; acp-proxy-core.el --- ACP-compatible core over ACP Proxy -*- lexical-binding: t; -*-

;; Copyright (C) 2025

;; Author: emacs-acp-proxy contributors
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))
;; Keywords: tools, processes
;; URL: https://github.com/nicehiro/emacs-acp-proxy

;;; Commentary:
;;
;; acp-proxy-core.el provides an ACP (Agent Client Protocol) compatible API
;; matching acp.el, but routes all traffic through the Rust ACP proxy server.
;; This lets existing libraries that depend on acp.el keep working while using
;; the proxy for process management and streaming.
;;
;; Load this file before any library that does (require 'acp). It provides
;; both 'acp and 'acp-proxy-core features.

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

(defconst acp-package-version "0.11.1-proxy-core")
(defconst acp--jsonrpc-version "2.0")

(defvar acp-logging-enabled nil)
(defvar acp-instance-count 0)

;; ---------------------------------------------------------------------------
;; Global shared proxy state
;; ---------------------------------------------------------------------------

(defvar acp-proxy-core--shared-connection nil
  "Shared jsonrpc connection to the proxy process.")

(defvar acp-proxy-core--shared-process nil
  "Shared proxy process.")

(defvar acp-proxy-core--shared-config-file nil
  "Path to the shared proxy config file.")

(defvar acp-proxy-core--shared-clients 0
  "Number of live clients attached to the shared proxy.")

(defvar acp-proxy-core--shared-agents (make-hash-table :test 'equal)
  "Hash table of agent specs keyed by agent name.")

(defvar acp-proxy-core--shared-active-agents (make-hash-table :test 'equal)
  "Agent names loaded in the currently running shared proxy.")

;; ---------------------------------------------------------------------------
;; Customization
;; ---------------------------------------------------------------------------

(defgroup acp-proxy-core nil
  "ACP-compatible core over ACP Proxy."
  :group 'tools
  :prefix "acp-proxy-core-")

(defcustom acp-proxy-core-proxy-program "emacs-acp-proxy"
  "Path to the ACP Proxy server binary."
  :type 'string
  :group 'acp-proxy-core)

(defcustom acp-proxy-core-log-level "info"
  "Log level for the ACP Proxy server."
  :type '(choice (const "trace")
          (const "debug")
          (const "info")
          (const "warn")
          (const "error"))
  :group 'acp-proxy-core)

(defcustom acp-proxy-core-log-file nil
  "Path to the ACP Proxy log file. When nil, log to stderr."
  :type '(choice (const :tag "Stderr (default)" nil)
          (file :tag "Log file path"))
  :group 'acp-proxy-core)

(defcustom acp-proxy-core-temp-dir
  (expand-file-name "acp-proxy-core/" temporary-file-directory)
  "Directory for generated proxy config files."
  :type 'directory
  :group 'acp-proxy-core)

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
PROXY-PROGRAM overrides `acp-proxy-core-proxy-program'.
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
        (cons :request-id 0)
        (cons :notification-handlers ())
        (cons :request-handlers ())
        (cons :error-handlers ())
        (cons :request-sender (or request-sender #'acp--request-sender))
        (cons :notification-sender (or notification-sender #'acp--notification-sender))
        (cons :request-resolver (or request-resolver #'acp--request-resolver))
        (cons :response-sender (or response-sender #'acp--response-sender))
        (cons :outgoing-request-decorator outgoing-request-decorator)
        (cons :proxy-program (or proxy-program acp-proxy-core-proxy-program))
        (cons :proxy-config-file proxy-config-file)
        (cons :agent-name agent-name)
        (cons :proxy-connected nil)
        (cons :connect-result nil)
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

(defun acp-proxy-core--agent-name (client)
  "Return agent name for CLIENT, deriving if needed."
  (or (map-elt client :agent-name)
      (when-let ((cmd (map-elt client :command)))
        (format "%s-%s"
                (file-name-nondirectory cmd)
                (map-elt client :instance-count)))))

(defun acp-proxy-core--env-alist (env-list)
  "Convert ENV-LIST of \"VAR=VAL\" strings to an alist."
  (let (out)
    (dolist (entry env-list (nreverse out))
      (when (string-match "\\`\\([^=]+\\)=\\(.*\\)\\'" entry)
        (push (cons (match-string 1 entry) (match-string 2 entry)) out)))))

(defun acp-proxy-core--agent-spec (client)
  "Return an agent spec plist for CLIENT."
  (list :command (map-elt client :command)
        :args (map-elt client :command-params)
        :env (acp-proxy-core--env-alist (map-elt client :environment-variables))))

(defun acp-proxy-core--register-agent (client)
  "Register CLIENT agent spec in the shared registry."
  (let* ((agent-name (acp-proxy-core--agent-name client))
         (spec (acp-proxy-core--agent-spec client))
         (existing (gethash agent-name acp-proxy-core--shared-agents)))
    (when (and existing (not (equal existing spec)))
      (error "Conflicting agent spec for %s in shared proxy" agent-name))
    (puthash agent-name spec acp-proxy-core--shared-agents)
    agent-name))

(defun acp-proxy-core--write-shared-config ()
  "Write shared proxy config for all registered agents."
  (let* ((dir (file-name-as-directory (expand-file-name acp-proxy-core-temp-dir)))
         (path (or acp-proxy-core--shared-config-file
                   (make-temp-file (expand-file-name "agents-" dir) nil ".toml"))))
    (make-directory dir t)
    (with-temp-buffer
      (insert (format "log_level = \"%s\"\n\n" acp-proxy-core-log-level))
      (maphash
       (lambda (agent-name spec)
         (let ((cmd (plist-get spec :command))
               (args (plist-get spec :args))
               (env (plist-get spec :env)))
           (insert (format "[agents.%s]\n" agent-name))
           (when cmd
             (insert (format "command = \"%s\"\n"
                             (replace-regexp-in-string "\\\"" "\\\\\"" cmd))))
           (when args
             (insert "args = [")
             (insert (mapconcat (lambda (a)
                                  (format "\"%s\"" (replace-regexp-in-string "\\\"" "\\\\\"" a)))
                                args
                                ", "))
             (insert "]\n"))
           (when env
             (insert "env = { ")
             (insert (mapconcat (lambda (pair)
                                  (format "\"%s\" = \"%s\""
                                          (car pair)
                                          (replace-regexp-in-string "\\\"" "\\\\\"" (cdr pair))))
                                env
                                ", "))
             (insert " }\n"))))
       acp-proxy-core--shared-agents)
      (write-region (point-min) (point-max) path nil 'quiet))
    (setq acp-proxy-core--shared-config-file path)
    path))

(defun acp-proxy-core--jsonrpc-request (client method params &optional callback error-callback)
  "Send JSON-RPC request to proxy for CLIENT." 
  (if (or callback error-callback)
      (jsonrpc-async-request
       (map-elt client :connection)
       (intern method)
       params
       :success-fn (or callback #'ignore)
       :error-fn (or error-callback #'ignore))
    (jsonrpc-request (map-elt client :connection) (intern method) params)))

(defun acp-proxy-core--connect-agent (client)
  "Connect the agent for CLIENT via the proxy."
  (let* ((agent-name (acp-proxy-core--agent-name client))
         (result (acp-proxy-core--jsonrpc-request
                  client
                  "acp/connectAgent"
                  (list :agentName agent-name))))
    (map-put! client :proxy-connected t)
    (map-put! client :connect-result result)
    result))

(defun acp-proxy-core--ensure-connected (client)
  "Ensure CLIENT is connected to its agent."
  (unless (map-elt client :proxy-connected)
    (acp-proxy-core--connect-agent client)))

(cl-defun acp--start-client (&key client)
  "Start CLIENT." 
  (unless client
    (error ":client is required"))
  (unless (or (map-elt client :command) (map-elt client :agent-name))
    (error ":command (or :agent-name) is required"))
  (when (and (not (map-elt client :command))
             (not (map-elt client :proxy-config-file)))
    (error ":proxy-config-file is required when :command is nil"))
  (when (acp--client-started-p client)
    (error "Client already started"))
  (acp-proxy-core--register-agent client)
  (if (and acp-proxy-core--shared-connection
           acp-proxy-core--shared-process
           (process-live-p acp-proxy-core--shared-process)
           (jsonrpc-running-p acp-proxy-core--shared-connection))
      (progn
        (let ((agent-name (acp-proxy-core--agent-name client)))
          (unless (gethash agent-name acp-proxy-core--shared-active-agents)
            (error "Agent %s not loaded in shared proxy; restart proxy to add it"
                   agent-name)))
        (setq acp-proxy-core--shared-clients (1+ acp-proxy-core--shared-clients))
        (map-put! client :connection acp-proxy-core--shared-connection)
        (map-put! client :process acp-proxy-core--shared-process))
    (let* ((proxy-program (map-elt client :proxy-program))
           (proxy-config (or (map-elt client :proxy-config-file)
                             (acp-proxy-core--write-shared-config)))
           (_ (setq acp-proxy-core--shared-config-file proxy-config))
           (_ (map-put! client :proxy-config-file proxy-config))
           (stderr-buffer (get-buffer-create "*acp-proxy-core-stderr(shared)*"))
           (stderr-proc (make-pipe-process
                         :name "acp-proxy-core-stderr(shared)"
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
           (args (append (list "--stdio" "--log-level" acp-proxy-core-log-level
                               "--config" proxy-config)
                         (when acp-proxy-core-log-file
                           (list "--log-file" acp-proxy-core-log-file))))
           (conn (jsonrpc-process-connection
                  :name "acp-proxy-core-shared"
                  :process
                  (lambda (_connection)
                    (make-process
                     :name "acp-proxy-core-shared"
                     :command (cons proxy-program args)
                     :connection-type 'pipe
                     :noquery t
                     :stderr stderr-proc))
                  :notification-dispatcher
                  (lambda (_conn method params)
                    (acp-proxy-core--jsonrpc-notification-dispatcher client method params))
                  :request-dispatcher
                  (lambda (_conn method params)
                    (acp-proxy-core--jsonrpc-request-dispatcher client method params))
                  :on-shutdown (lambda (connection)
                                 (acp-proxy-core--process-sentinel client connection)))))
      (setq acp-proxy-core--shared-connection conn)
      (setq acp-proxy-core--shared-process (jsonrpc--process conn))
      (setq acp-proxy-core--shared-clients 1)
      (clrhash acp-proxy-core--shared-active-agents)
      (maphash (lambda (k _v) (puthash k t acp-proxy-core--shared-active-agents))
               acp-proxy-core--shared-agents)
      (map-put! client :connection acp-proxy-core--shared-connection)
      (map-put! client :process acp-proxy-core--shared-process))))

(defun acp-proxy-core--process-sentinel (client connection)
  "Handle proxy process shutdown for CONNECTION." 
  (let* ((proc (jsonrpc--process connection))
         (event (cond
                 ((and proc (not (process-live-p proc)))
                  (format "exited (%s)" (process-status proc)))
                 (t "stopped"))))
    (acp--log client "PROCESS" "%s" event)
    (setq acp-proxy-core--shared-connection nil)
    (setq acp-proxy-core--shared-process nil)
    (setq acp-proxy-core--shared-clients 0)
    (clrhash acp-proxy-core--shared-active-agents)
    (map-put! client :proxy-connected nil)
    (map-put! client :connection nil)
    (map-put! client :process nil)))

(cl-defun acp-shutdown (&key client)
  "Shutdown ACP CLIENT and release resources." 
  (unless client
    (error ":client is required"))
  (when (acp--client-started-p client)
    (setq acp-proxy-core--shared-clients (max 0 (1- acp-proxy-core--shared-clients)))
    (when (and (<= acp-proxy-core--shared-clients 0)
               acp-proxy-core--shared-connection)
      (jsonrpc-shutdown acp-proxy-core--shared-connection))
    (map-put! client :proxy-connected nil)
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

(defun acp-proxy-core--get (obj key)
  "Get KEY from OBJ supporting plist/alist/hash with keyword or symbol keys." 
  (let* ((k1 key)
         (k2 (if (keywordp key) (intern (substring (symbol-name key) 1)) (intern (format ":%s" key)))))
    (cond
     ((hash-table-p obj) (or (gethash k1 obj) (gethash k2 obj)))
     ((and (listp obj) (keywordp (car obj))) (or (plist-get obj k1) (plist-get obj k2)))
     ((listp obj) (or (cdr (assoc k1 obj)) (cdr (assoc k2 obj))))
     (t nil))))

(defun acp-proxy-core--vector-of-chars-p (value)
  "Return non-nil if VALUE is a vector of character codes." 
  (and (vectorp value)
       (> (length value) 0)
       (seq-every-p #'integerp value)))

(defun acp-proxy-core--normalize-prompt (prompt)
  "Normalize ACP prompt value to proxy-compatible message." 
  (cond
   ((stringp prompt) prompt)
   ((acp-proxy-core--vector-of-chars-p prompt)
    (apply #'string (append prompt nil)))
   (t prompt)))

(defun acp-proxy-core--map-request (client request)
  "Return (METHOD PARAMS RESULT-MAPPER) for proxy request." 
  (let* ((method (map-elt request :method))
         (params (map-elt request :params)))
    (pcase method
      ("initialize"
       (list "acp/connectAgent"
             (list :agentName (acp-proxy-core--agent-name client))
             (lambda (result) result)))
      ("authenticate"
       (let ((method-id (acp-proxy-core--get params 'methodId)))
         (list "acp/authenticate"
               (list :agentName (acp-proxy-core--agent-name client)
                     :authMethodId method-id)
               (lambda (result) result))))
      ("session/new"
       (let ((cwd (acp-proxy-core--get params 'cwd)))
         (list "acp/newSession"
               (list :agentName (acp-proxy-core--agent-name client)
                     :cwd cwd
                     :mcpServers (or (acp-proxy-core--get params 'mcpServers) [])
                     :_meta (acp-proxy-core--get params '_meta))
               (lambda (result) result))))
      ("session/prompt"
       (let ((session-id (acp-proxy-core--get params 'sessionId))
             (prompt (or (acp-proxy-core--get params 'prompt)
                         (acp-proxy-core--get params 'message))))
         (list "acp/prompt"
               (list :sessionId session-id
                     :message (acp-proxy-core--normalize-prompt prompt))
               (lambda (result) result))))
      ("session/set_mode"
       (list "acp/setMode"
             (list :sessionId (acp-proxy-core--get params 'sessionId)
                   :modeId (acp-proxy-core--get params 'modeId))
             (lambda (result) result)))
      ("session/set_model"
       (list "acp/setModel"
             (list :sessionId (acp-proxy-core--get params 'sessionId)
                   :modelId (acp-proxy-core--get params 'modelId))
             (lambda (result) result)))
      ("session/cancel"
       (list "acp/cancel"
             (list :sessionId (acp-proxy-core--get params 'sessionId)
                   :reason (acp-proxy-core--get params 'reason))
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
               (acp-proxy-core--map-request client request)))
    (unless proxy-method
      (let ((err (acp-make-error :code -32601 :message (format "Method not supported: %s" (map-elt request :method)))))
        (if on-failure
            (with-temp-buffer
              (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                (funcall on-failure err)))
          (error "ACP request failed: %s" err)))
      (cl-return-from acp--request-sender nil))
    (when (string= proxy-method "acp/connectAgent")
      (acp-proxy-core--ensure-connected client)
      (let ((result (map-elt client :connect-result)))
        (if sync
            (cl-return-from acp--request-sender (if result-mapper (funcall result-mapper result) result))
          (when on-success
            (with-temp-buffer
              (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                (funcall on-success (if result-mapper (funcall result-mapper result) result)))))
          (cl-return-from acp--request-sender nil))))
    (acp-proxy-core--ensure-connected client)
    (if sync
        (condition-case err
            (let* ((result (acp-proxy-core--jsonrpc-request client proxy-method proxy-params))
                   (mapped (if result-mapper (funcall result-mapper result) result)))
              mapped)
          (error
           (let ((err-obj `((code . -32603) (message . ,(error-message-string err)))))
             (if on-failure
                 (with-temp-buffer
                   (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
                     (funcall on-failure err-obj)))
               (error "ACP request failed: %s" err-obj)))))
      (acp-proxy-core--jsonrpc-request
       client
       proxy-method
       proxy-params
       (when on-success
         (lambda (result)
           (with-temp-buffer
             (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
               (funcall on-success (if result-mapper (funcall result-mapper result) result))))))
       (when on-failure
         (lambda (err)
           (with-temp-buffer
             (with-current-buffer (or buffer (map-elt client :context-buffer) (current-buffer))
               (funcall on-failure err))))))))

(cl-defun acp--notification-sender (&key client notification sync)
  "Send NOTIFICATION from CLIENT via proxy." 
  (let* ((method (map-elt notification :method))
         (params (map-elt notification :params)))
    (pcase method
      ("session/cancel"
       (if sync
           (acp-proxy-core--jsonrpc-request
            client "acp/cancel"
            (list :sessionId (acp-proxy-core--get params 'sessionId)
                  :reason (acp-proxy-core--get params 'reason)))
         (acp-proxy-core--jsonrpc-request
          client "acp/cancel"
          (list :sessionId (acp-proxy-core--get params 'sessionId)
                :reason (acp-proxy-core--get params 'reason))
          #'ignore #'ignore)))
      (_
       (acp--log client "NOTIFICATION" "Unsupported notification: %s" method)))))

(cl-defun acp-send-response (&key client response)
  "Send a request RESPONSE from CLIENT." 
  (unless client
    (error ":client is required"))
  (unless response
    (error ":response is required"))
  (funcall (map-elt client :response-sender)
           :client client
           :response response))

(defun acp-proxy-core--extract-permission-outcome (response)
  "Extract (OUTCOME OPTION-ID) from RESPONSE." 
  (let* ((result (map-elt response :result))
         (outcome (or (acp-proxy-core--get result 'outcome) result))
         (outcome-type (or (acp-proxy-core--get outcome 'outcome)
                           (acp-proxy-core--get outcome 'status)))
         (option-id (acp-proxy-core--get outcome 'optionId)))
    (list outcome-type option-id)))

(defun acp-proxy-core--select-option-id (options)
  "Pick a best-effort option id from OPTIONS list." 
  (let ((opts (if (vectorp options) (append options nil) options))
        (fallback nil))
    (dolist (opt opts)
      (let* ((opt-id (or (acp-proxy-core--get opt 'id)
                         (acp-proxy-core--get opt 'optionId)))
             (label (or (acp-proxy-core--get opt 'label)
                        (acp-proxy-core--get opt 'title)
                        "")))
        (setq fallback (or fallback opt-id))
        (when (and opt-id (string-match-p "\`\(reject\|deny\|cancel\)\'" (downcase opt-id)))
          (cl-return-from acp-proxy-core--select-option-id opt-id))
        (when (and opt-id (string-match-p "reject\|deny\|cancel" (downcase label)))
          (cl-return-from acp-proxy-core--select-option-id opt-id))))
    fallback))

(cl-defun acp--response-sender (&key client response)
  "Send a request RESPONSE from CLIENT." 
  (let* ((request-id (map-elt response :request-id))
         (pending (and request-id
                       (gethash request-id (map-elt client :pending-permission-requests)))))
    (if (not pending)
        (acp--log client "RESPONSE" "Unhandled response id: %s" request-id)
      (let* ((outcome (acp-proxy-core--extract-permission-outcome response))
             (outcome-type (car outcome))
             (option-id (cadr outcome))
             (options (plist-get pending :options))
             (option-id (or option-id
                            (when (and outcome-type (string= outcome-type "cancelled"))
                              (acp-proxy-core--select-option-id options))
                            (acp-proxy-core--select-option-id options))))
        (remhash request-id (map-elt client :pending-permission-requests))
        (acp-proxy-core--jsonrpc-request
         client
         "acp/respondPermission"
         (list :requestId (plist-get pending :requestId)
               :sessionId (plist-get pending :sessionId)
               :optionId option-id)
         #'ignore #'ignore)))))

;; ---------------------------------------------------------------------------
;; Incoming proxy notifications -> ACP-compatible dispatch
;; ---------------------------------------------------------------------------

(defun acp-proxy-core--dispatch-notification (client method params)
  "Dispatch an ACP-compatible notification to CLIENT handlers." 
  (let ((notification `((jsonrpc . ,acp--jsonrpc-version)
                        (method . ,method)
                        (params . ,params))))
    (dolist (handler (map-elt client :notification-handlers))
      (condition-case-unless-debug err
          (funcall handler notification)
        (error (acp--log client "NOTIFICATION HANDLER ERROR" "%S" err))))))

(defun acp-proxy-core--dispatch-request (client method params request-id)
  "Dispatch an ACP-compatible request to CLIENT handlers." 
  (let ((request `((jsonrpc . ,acp--jsonrpc-version)
                   (method . ,method)
                   (id . ,request-id)
                   (params . ,params))))
    (dolist (handler (map-elt client :request-handlers))
      (condition-case-unless-debug err
          (funcall handler request)
        (error (acp--log client "REQUEST HANDLER ERROR" "%S" err))))))

(defun acp-proxy-core--jsonrpc-notification-dispatcher (client method params)
  "Handle incoming JSON-RPC notification METHOD with PARAMS." 
  (pcase (if (symbolp method) (symbol-name method) method)
    ("acp/sessionUpdate"
     (acp-proxy-core--dispatch-notification
      client "session/update" params))
    ("acp/permissionRequest"
     (let* ((request-id (format "perm-%s" (map-elt client :request-id)))
            (session-id (acp-proxy-core--get params 'sessionId))
            (proxy-request-id (acp-proxy-core--get params 'requestId))
            (options (acp-proxy-core--get params 'options)))
       (map-put! client :request-id (1+ (map-elt client :request-id)))
       (puthash request-id
                (list :requestId proxy-request-id
                      :sessionId session-id
                      :options options)
                (map-elt client :pending-permission-requests))
       (acp-proxy-core--dispatch-request
        client
        "session/request_permission"
        params
        request-id)))
    ("acp/authRequired"
     (acp-proxy-core--dispatch-notification client "auth/required" params))
    ("acp/agentDisconnected"
     (acp-proxy-core--dispatch-notification client "agent/disconnected" params))
    ("acp/fileChanged"
     (acp-proxy-core--dispatch-notification client "fs/file_changed" params))
    (_
     (acp-proxy-core--dispatch-notification client (acp-proxy-core--normalize-method method) params))))

(defun acp-proxy-core--jsonrpc-request-dispatcher (_client method _params)
  "Handle incoming JSON-RPC request METHOD from proxy.
Proxy does not send requests; this is an error." 
  (error "Unsupported request from proxy: %s" method))

(defun acp-proxy-core--normalize-method (method)
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
  "Instantiate an "initialize" request." 
  (unless protocol-version
    (error ":protocol-version is required"))
  `((:method . "initialize")
    (:params . (,@(when client-info
                    `((clientInfo . ,client-info)))
                (protocolVersion . ,protocol-version)
                (clientCapabilities . ((fs . ((readTextFile . ,(if read-text-file-capability t :false))
                                              (writeTextFile . ,(if write-text-file-capability t :false))))))))))

(cl-defun acp-make-authenticate-request (&key method-id method)
  "Instantiate an "authenticate" request." 
  (unless method-id
    (error ":method-id is required"))
  `((:method . "authenticate")
    (:params . ,(append `((methodId . ,method-id))
                        (when method
                          `((authMethod . ,method)))))))

(cl-defun acp-make-session-new-request (&key cwd mcp-servers meta)
  "Instantiate a "session/new" request." 
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/new")
    (:params . ((cwd . ,(directory-file-name (expand-file-name cwd)))
                (mcpServers . ,(or mcp-servers []))
                ,@(when meta `((_meta . ,meta)))))))

(cl-defun acp-make-session-prompt-request (&key session-id prompt)
  "Instantiate a "session/prompt" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless prompt
    (error ":prompt is required"))
  `((:method . "session/prompt")
    (:params . ((sessionId . ,session-id)
                (prompt . ,(vconcat prompt))))))

(cl-defun acp-make-session-set-mode-request (&key session-id mode-id)
  "Instantiate a "session/set_mode" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless mode-id
    (error ":mode-id is required"))
  `((:method . "session/set_mode")
    (:params . ((sessionId . ,session-id)
                (modeId . ,mode-id)))))

(cl-defun acp-make-session-set-model-request (&key session-id model-id)
  "Instantiate a "session/set_model" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless model-id
    (error ":model-id is required"))
  `((:method . "session/set_model")
    (:params . ((sessionId . ,session-id)
                (modelId . ,model-id)))))

(cl-defun acp-make-session-resume-request (&key session-id cwd mcp-servers)
  "Instantiate a "session/resume" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/resume")
    (:params . ((sessionId . ,session-id)
                (cwd . ,(directory-file-name (expand-file-name cwd)))
                (mcpServers . ,(or mcp-servers []))))))

(cl-defun acp-make-session-list-request (&key cwd)
  "Instantiate a "session/list" request." 
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/list")
    (:params . ((cwd . ,(directory-file-name (expand-file-name cwd)))))))

(cl-defun acp-make-session-load-request (&key session-id cwd mcp-servers)
  "Instantiate a "session/load" request." 
  (unless session-id
    (error ":session-id is required"))
  (unless cwd
    (error ":cwd is required"))
  `((:method . "session/load")
    (:params . ((sessionId . ,session-id)
                (cwd . ,(directory-file-name (expand-file-name cwd)))
                (mcpServers . ,(or mcp-servers []))))))

(cl-defun acp-make-session-delete-request (&key session-id)
  "Instantiate a "session/delete" request." 
  (unless session-id
    (error ":session-id is required"))
  `((:method . "session/delete")
    (:params . ((sessionId . ,session-id)))))

(cl-defun acp-make-session-request-permission-response (&key request-id option-id cancelled)
  "Instantiate a "session/request_permission" response." 
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
  "Instantiate a "fs/read_text_file" response." 
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
  "Instantiate a "fs/write_text_file" response." 
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
  "Instantiate a "session/cancel" request." 
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

(provide 'acp)

;;; acp-proxy-core.el ends here
