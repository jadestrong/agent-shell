;;; agent-shell-ui-tests.el --- Tests for agent-shell-ui -*- lexical-binding: t; -*-

(require 'ert)
(require 'agent-shell-ui)

;;; Code:

(ert-deftest agent-shell-ui-body-invisible-p-handles-whitespace-only-body ()
  ;; Regression for PR #597 (pi-acp): the markdown renderer strips
  ;; an empty `\\`\\`\\`console' fence down to a body of only
  ;; newlines.  On the next `agent-shell-ui--replace-body',
  ;; `--body-invisible-p' must still report the body as hidden when
  ;; its chars carry `invisible t' — otherwise new chars come in
  ;; visible and the fragment "expands" on every subsequent update
  ;; while still showing the `▶' collapsed indicator.
  (with-temp-buffer
    (insert "\n\n")
    (add-text-properties (point-min) (point-max) '(invisible t))
    (should (agent-shell-ui--body-invisible-p (point-min) (point-max))))
  (with-temp-buffer
    (insert "\n\n")
    (should-not (agent-shell-ui--body-invisible-p (point-min) (point-max)))))

(ert-deftest agent-shell-ui-indent-text-preserves-caller-text-properties ()
  ;; A pre-rendered body (eg. a diff tagged `agent-shell-markdown-frozen')
  ;; passes through `--indent-text' on its way into the fragment buffer.
  ;; Every char of the indented result — including the inter-line `\\n's
  ;; — must keep the caller's text properties, otherwise the markdown
  ;; renderer's contiguous frozen-range collapses per-line and the
  ;; header / blockquote passes match across the now-bare line breaks.
  ;; See PR #597.
  (let* ((input (propertize "line one\nline two\nline three"
                            'agent-shell-markdown-frozen t))
         (out (agent-shell-ui--indent-text input "  ")))
    (dotimes (i (length out))
      (should (eq t (get-text-property i 'agent-shell-markdown-frozen out)))
      (should (equal "  " (get-text-property i 'line-prefix out)))
      (should (equal "  " (get-text-property i 'wrap-prefix out))))))

(defun agent-shell-ui-tests--make-buffer-with-fragments (fragments)
  "Create a temp buffer with FRAGMENTS inserted.

FRAGMENTS is a list of alists, each with keys :namespace-id,
:block-id, :label-left, :body, and optionally :expanded.

Example:

  (agent-shell-ui-tests--make-buffer-with-fragments
   \\='(((:namespace-id . \"ns\") (:block-id . \"1\")
      (:label-left . \"First\") (:body . \"body one\")
      (:expanded . t))
     ((:namespace-id . \"ns\") (:block-id . \"2\")
      (:label-left . \"Second\") (:body . \"body two\"))))

Returns the buffer.  Caller must kill it."
  (let ((buf (generate-new-buffer " *test-ui-fragments*")))
    (with-current-buffer buf
      (agent-shell-ui-mode 1)
      (dolist (frag fragments)
        (agent-shell-ui-update-fragment
         (agent-shell-ui-make-fragment-model
          :namespace-id (map-elt frag :namespace-id)
          :block-id (map-elt frag :block-id)
          :label-left (map-elt frag :label-left)
          :label-right (map-elt frag :label-right)
          :body (map-elt frag :body))
         :expanded (map-elt frag :expanded)
         :navigation 'always)))
    buf))

(defun agent-shell-ui-tests--fragment-collapsed-p (namespace-id block-id)
  "Return non-nil when fragment NAMESPACE-ID/BLOCK-ID is collapsed."
  (let ((qualified-id (format "%s-%s" namespace-id block-id)))
    (save-mark-and-excursion
      (goto-char (point-min))
      (when-let* ((match (text-property-search-forward
                         'agent-shell-ui-state nil
                         (lambda (_ state)
                           (equal (map-elt state :qualified-id) qualified-id))
                         t)))
        (map-elt (get-text-property (prop-match-beginning match)
                                    'agent-shell-ui-state)
                 :collapsed)))))

;;; majority-collapsed-p

(ert-deftest agent-shell-ui-majority-collapsed-all-collapsed-test ()
  "All collapsed fragments yields non-nil."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a"))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "B") (:body . "body b"))
                ((:namespace-id . "ns") (:block-id . "3")
                 (:label-left . "C") (:body . "body c"))))))
    (unwind-protect
        (with-current-buffer buf
          (should (agent-shell-ui--majority-collapsed-p)))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-majority-collapsed-all-expanded-test ()
  "All expanded fragments yields nil."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "B") (:body . "body b") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "3")
                 (:label-left . "C") (:body . "body c") (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          (should-not (agent-shell-ui--majority-collapsed-p)))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-majority-collapsed-mixed-test ()
  "Three collapsed, two expanded yields non-nil."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a"))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "B") (:body . "body b") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "3")
                 (:label-left . "C") (:body . "body c"))
                ((:namespace-id . "ns") (:block-id . "4")
                 (:label-left . "D") (:body . "body d") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "5")
                 (:label-left . "E") (:body . "body e"))))))
    (unwind-protect
        (with-current-buffer buf
          (should (agent-shell-ui--majority-collapsed-p)))
      (kill-buffer buf))))

;;; toggle-all-fragments

(ert-deftest agent-shell-ui-toggle-all-collapses-expanded-test ()
  "Toggling when all expanded collapses everything."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "B") (:body . "body b") (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          (agent-shell-ui-toggle-all-fragments)
          (should (agent-shell-ui-tests--fragment-collapsed-p "ns" "1"))
          (should (agent-shell-ui-tests--fragment-collapsed-p "ns" "2"))
          (should (eq agent-shell-ui--fold-toggle-state 'collapsed)))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-toggle-all-expands-collapsed-test ()
  "Toggling when all collapsed expands everything."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a"))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "B") (:body . "body b"))))))
    (unwind-protect
        (with-current-buffer buf
          (agent-shell-ui-toggle-all-fragments)
          (should-not (agent-shell-ui-tests--fragment-collapsed-p "ns" "1"))
          (should-not (agent-shell-ui-tests--fragment-collapsed-p "ns" "2"))
          (should (eq agent-shell-ui--fold-toggle-state 'expanded)))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-toggle-all-round-trip-test ()
  "Toggling twice returns to original state."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "B") (:body . "body b") (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          ;; First toggle: collapse all
          (agent-shell-ui-toggle-all-fragments)
          (should (agent-shell-ui-tests--fragment-collapsed-p "ns" "1"))
          ;; Second toggle: expand all
          (agent-shell-ui-toggle-all-fragments)
          (should-not (agent-shell-ui-tests--fragment-collapsed-p "ns" "1"))
          (should-not (agent-shell-ui-tests--fragment-collapsed-p "ns" "2")))
      (kill-buffer buf))))

;;; enclosing-fragment-position

(ert-deftest agent-shell-ui-enclosing-position-on-fragment-test ()
  "When point is on a fragment, return point."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a") (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          ;; Move to a position that has agent-shell-ui-state
          (goto-char (point-min))
          (text-property-search-forward 'agent-shell-ui-state nil
                                        (lambda (_ s) (and s t)) t)
          (goto-char (prop-match-beginning
                      (save-mark-and-excursion
                        (text-property-search-backward
                         'agent-shell-ui-state nil
                         (lambda (_ s) (and s t)) t))))
          (should (equal (agent-shell-ui--enclosing-fragment-position)
                         (point))))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-enclosing-position-nil-in-empty-buffer-test ()
  "Empty buffer returns nil."
  (let ((buf (generate-new-buffer " *test-ui-empty*")))
    (unwind-protect
        (with-current-buffer buf
          (agent-shell-ui-mode 1)
          (should-not (agent-shell-ui--enclosing-fragment-position)))
      (kill-buffer buf))))

;;; toggle-fragment

(ert-deftest agent-shell-ui-toggle-fragment-on-fragment-test ()
  "Toggle on a fragment toggles it."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "A") (:body . "body a") (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          ;; Position on the fragment
          (goto-char (point-min))
          (text-property-search-forward 'agent-shell-ui-state nil
                                        (lambda (_ s) (and s t)) t)
          (goto-char (prop-match-beginning
                      (save-mark-and-excursion
                        (text-property-search-backward
                         'agent-shell-ui-state nil
                         (lambda (_ s) (and s t)) t))))
          ;; Fragment starts expanded, toggle should collapse it
          (agent-shell-ui-toggle-fragment)
          (should (agent-shell-ui-tests--fragment-collapsed-p "ns" "1")))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-toggle-survives-surgical-replace-test ()
  "Toggle target stays consistent after `--surgical-replace-body'.

Surgical replace mints a fresh state plist on the new body chars
but `:qualified-id` is stable.  Toggle resolves the target via
`:qualified-id` so it still hits the right fragment."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "Tool") (:body . "initial")
                 (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          (agent-shell-ui-update-fragment
           (agent-shell-ui-make-fragment-model
            :namespace-id "ns" :block-id "1"
            :body "replaced body content")
           :append nil :navigation 'always)
          (goto-char (point-min))
          (text-property-search-forward 'agent-shell-ui-state nil
                                        (lambda (_ s) (and s t)) t)
          (goto-char (prop-match-beginning
                      (save-mark-and-excursion
                        (text-property-search-backward
                         'agent-shell-ui-state nil
                         (lambda (_ s) (and s t)) t))))
          (agent-shell-ui-toggle-fragment)
          (should (agent-shell-ui-tests--fragment-collapsed-p "ns" "1")))
      (kill-buffer buf))))

;;; backward-block

(defun agent-shell-ui-tests--fragment-start (qualified-id)
  "Return the start position of fragment QUALIFIED-ID, or nil."
  (save-mark-and-excursion
    (goto-char (point-min))
    (when-let* ((match (text-property-search-forward
                        'agent-shell-ui-state nil
                        (lambda (_ state)
                          (equal (map-elt state :qualified-id) qualified-id))
                        t)))
      (prop-match-beginning match))))

(ert-deftest agent-shell-ui-backward-block-from-inside-goes-to-own-start-test ()
  "`agent-shell-ui-backward-block' from inside a block goes to its own start.

From the block's start it then jumps to the previous block."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "First") (:body . "body one") (:expanded . t))
                ((:namespace-id . "ns") (:block-id . "2")
                 (:label-left . "Second") (:body . "body two") (:expanded . t))))))
    (unwind-protect
        (with-current-buffer buf
          (let ((first-start (agent-shell-ui-tests--fragment-start "ns-1"))
                (second-start (agent-shell-ui-tests--fragment-start "ns-2")))
            ;; Strictly inside the second block -> its own start.
            (goto-char (+ second-start 3))
            (should (equal (agent-shell-ui-backward-block) second-start))
            ;; At the second block's start -> the previous block.
            (goto-char second-start)
            (should (equal (agent-shell-ui-backward-block) first-start))))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-backward-block-skips-non-navigatable-block-test ()
  "`agent-shell-ui-backward-block' skips non-navigatable blocks.

From inside a non-navigatable block it lands on the previous
navigatable block, not on the non-navigatable block's own start."
  (let ((buf (generate-new-buffer " *test-ui-fragments*")))
    (unwind-protect
        (with-current-buffer buf
          (agent-shell-ui-mode 1)
          (agent-shell-ui-update-fragment
           (agent-shell-ui-make-fragment-model
            :namespace-id "ns" :block-id "1"
            :label-left "First" :body "body one")
           :expanded t :navigation 'always)
          (agent-shell-ui-update-fragment
           (agent-shell-ui-make-fragment-model
            :namespace-id "ns" :block-id "2"
            :label-left "Second" :body "body two")
           :expanded t :navigation 'never)
          (let ((first-start (agent-shell-ui-tests--fragment-start "ns-1"))
                (non-nav-start (agent-shell-ui-tests--fragment-start "ns-2")))
            (goto-char (+ non-nav-start 3))
            (should (equal (agent-shell-ui-backward-block) first-start))))
      (kill-buffer buf))))

(ert-deftest agent-shell-ui-label-length-change-preserves-body-collapse ()
  ;; Regression: a collapsed fragment that already has a body gets an
  ;; update where the label-left changes length (e.g. the status label
  ;; growing from a short kind label to "[ completed ]") together with a
  ;; new body.  `agent-shell-ui-update-fragment' must derive the body
  ;; range *after* rewriting the labels — a range captured beforehand is
  ;; shifted by the label-length delta and points into the (visible)
  ;; label-right, so `--replace-body' eats the label↔body boundary and
  ;; inserts the body visible under a collapsed `▶' indicator.  This is
  ;; most visible on `execute' (run) tool calls, whose status label grows
  ;; on completion.
  (cl-flet ((section-text (section)
              (when-let* ((r (agent-shell-ui--nearest-range-matching-property
                              :property 'agent-shell-ui-section :value section
                              :from (point-min) :to (point-max))))
                (buffer-substring-no-properties (map-elt r :start) (map-elt r :end))))
            (body-invisible-p ()
              (when-let* ((r (agent-shell-ui--nearest-range-matching-property
                              :property 'agent-shell-ui-section :value 'body
                              :from (point-min) :to (point-max))))
                (eq (get-text-property (map-elt r :start) 'invisible) t))))
    (with-temp-buffer
      ;; Labels-only, collapsed.
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id 1 :block-id "tc"
        :label-left "[p]" :label-right "RIGHT-LABEL" :body nil)
       :expanded nil :no-undo t)
      ;; Body arrives, still collapsed.
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id 1 :block-id "tc"
        :label-left "[p]" :label-right "RIGHT-LABEL" :body "FIRST BODY")
       :expanded nil :no-undo t)
      (should (body-invisible-p))
      (should (equal (section-text 'label-right) "RIGHT-LABEL"))
      ;; Completion: label-left grows (delta exceeds the label↔body
      ;; separator) and the body is replaced.
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id 1 :block-id "tc"
        :label-left "[ a much longer completed status label ]"
        :label-right "RIGHT-LABEL" :body "SECOND BODY")
       :expanded nil :no-undo t)
      ;; Label-right must survive intact (not partially deleted).
      (should (equal (section-text 'label-right) "RIGHT-LABEL"))
      ;; Body must stay collapsed (invisible) to match the `▶' indicator.
      (should (body-invisible-p))
      ;; And the replacement body content must be present.
      (when-let* ((r (agent-shell-ui--nearest-range-matching-property
                      :property 'agent-shell-ui-section :value 'body
                      :from (point-min) :to (point-max))))
        (should (string-match-p
                 "SECOND BODY"
                 (buffer-substring-no-properties (map-elt r :start)
                                                 (map-elt r :end))))))))

(provide 'agent-shell-ui-tests)

;;; agent-shell-ui-tests.el ends here
