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
