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

(defun agent-shell-ui-tests--visible-body-p ()
  "Return non-nil if any body-section char in the buffer is visible.
A collapsed fragment must keep every body char `invisible'."
  (save-mark-and-excursion
    (goto-char (point-min))
    (catch 'visible
      (while (< (point) (point-max))
        (when (and (eq (get-text-property (point) 'agent-shell-ui-section) 'body)
                   (not (get-text-property (point) 'invisible)))
          (throw 'visible t))
        (goto-char (or (next-single-property-change (point) 'agent-shell-ui-section)
                       (point-max))))
      nil)))

(ert-deftest agent-shell-ui-body-stays-collapsed-after-label-length-change-test ()
  "A collapsed body stays hidden when a label update changes label length.

A combined label+body update replaces the label first, which can change
its length and shift the body below it.  Deriving the body range before
that replacement leaves it stale, so `--replace-body' corrupts the body
boundary and leaks the collapsed content into view (e.g. a diff spilling
out of a collapsed edit tool call).  The label-right sits right above the
body, so growing it shifts the body the most.  The body must stay
invisible across the update."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "1")
                 (:label-left . "Edit") (:label-right . "short")
                 (:body . "first body\nsecond line"))))))
    (unwind-protect
        (with-current-buffer buf
          ;; Sanity: the body starts collapsed (hidden).
          (should-not (agent-shell-ui-tests--visible-body-p))
          ;; Grow the adjacent label-right and replace the body at once.
          (agent-shell-ui-update-fragment
           (agent-shell-ui-make-fragment-model
            :namespace-id "ns" :block-id "1"
            :label-left "Edit" :label-right "a much longer right label than before"
            :body "second body content\nanother line")
           :append nil :navigation 'always)
          (should-not (agent-shell-ui-tests--visible-body-p)))
      (kill-buffer buf))))

;;; delete-fragment

(ert-deftest agent-shell-ui-delete-fragment-preserves-next-indicator-test ()
  "Deleting a fragment keeps the following fragment's leading indicator.

A collapsed labels-only fragment reserves a two-space indicator
placeholder for column alignment.  Deleting the fragment right above it
must not consume that placeholder.  Regression: a permission dialog
deleted on tool-call completion swallowed the next tool call's indent
because `agent-shell-ui-delete-fragment' skipped trailing whitespace
straight into the next block's leading spaces."
  (let ((buf (agent-shell-ui-tests--make-buffer-with-fragments
              '(((:namespace-id . "ns") (:block-id . "top")
                 (:label-left . "Top"))
                ((:namespace-id . "ns") (:block-id . "next")
                 (:label-left . "Next"))))))
    (unwind-protect
        (with-current-buffer buf
          (agent-shell-ui-delete-fragment :namespace-id "ns" :block-id "top")
          (let ((start (agent-shell-ui-tests--fragment-start "ns-next")))
            (should start)
            (should (equal "  " (buffer-substring-no-properties start (+ start 2))))))
      (kill-buffer buf))))

;;; groups

(defun agent-shell-ui-tests--group-child-ids (group-qualified-id)
  "Return the ordered member qualified-ids of GROUP-QUALIFIED-ID."
  (mapcar (lambda (c) (map-elt c :qualified-id))
          (agent-shell-ui--group-children :group-qualified-id group-qualified-id)))

(ert-deftest agent-shell-ui-group-auto-creates-header-and-nests-members-test ()
  "A member with a `:group-id' auto-creates the header and nests under it."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    (dolist (id '("t1" "t2"))
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id "ns" :block-id id :group-id "grp" :group-label "Tools"
        :label-left "run" :label-right id)
       :navigation 'always))
    (should (agent-shell-ui--group-header-range "ns-grp"))
    (should (equal '("ns-t1" "ns-t2")
                   (agent-shell-ui-tests--group-child-ids "ns-grp")))))

(ert-deftest agent-shell-ui-group-reports-created-header-range-test ()
  "Creating a group header reports its range; a later member reports none.
The header is inserted on its own, outside any member's block/padding, so
`agent-shell-ui-update-fragment' must hand its extent back to the caller
via `:group-header' (callers mark output over that span so navigation
does not stop mid-header).  The span covers the header and its padding,
and only the header-creating call reports it."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    (let* ((first (agent-shell-ui-update-fragment
                   (agent-shell-ui-make-fragment-model
                    :namespace-id "ns" :block-id "t1" :group-id "grp"
                    :group-label "Tools" :label-left "run" :label-right "t1")
                   :navigation 'always))
           (second (agent-shell-ui-update-fragment
                    (agent-shell-ui-make-fragment-model
                     :namespace-id "ns" :block-id "t2" :group-id "grp"
                     :group-label "Tools" :label-left "run" :label-right "t2")
                    :navigation 'always))
           (header (agent-shell-ui--group-header-range "ns-grp"))
           (gh-start (map-nested-elt first '(:group-header :start)))
           (gh-end (map-nested-elt first '(:group-header :end))))
      ;; First member (which materialized the header) reports its range.
      (should gh-start)
      (should gh-end)
      ;; Span encloses the header block itself.
      (should (<= gh-start (map-elt header :start)))
      (should (>= gh-end (map-elt header :end)))
      ;; A second member into the same group creates no header, reports none.
      (should-not (map-elt second :group-header)))))

(ert-deftest agent-shell-ui-group-member-padding-abuts-following-block-test ()
  "A grouped member's padding reaches the following top-level block's padding.
The group's trailing separator (the header's `\\n\\n') is pushed below the
member, belonging to neither the header block nor the member.  The member
must fold it into its padding so the reported ranges tile with no gap;
otherwise that separator is left outside every block's range (and a
caller stamping ranges leaves it unmarked, stranding navigation there)."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    (let* ((member (agent-shell-ui-update-fragment
                    (agent-shell-ui-make-fragment-model
                     :namespace-id "0" :block-id "th" :label-left "Thinking"
                     :body "pondering" :group-id "grp" :group-label "Activity")
                    :create-new t :expanded t))
           (top (agent-shell-ui-update-fragment
                 (agent-shell-ui-make-fragment-model
                  :namespace-id "0" :block-id "msg" :body "Answer")
                 :create-new t)))
      (should (= (map-nested-elt member '(:padding :end))
                 (map-nested-elt top '(:padding :start)))))))

(ert-deftest agent-shell-ui-group-collapse-hides-members-and-restores-state-test ()
  "Collapsing a group hides every member; expanding restores per-member folds."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    ;; m1 stays collapsed (default), m2 expanded.
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "grp" :group-label "T"
      :label-left "run" :label-right "a" :body "aa")
     :navigation 'always)
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m2" :group-id "grp" :group-label "T"
      :label-left "run" :label-right "b" :body "bb")
     :expanded t :navigation 'always)
    (cl-flet ((member-start (n) (map-elt (nth n (agent-shell-ui--group-children
                                                 :group-qualified-id "ns-grp"))
                                         :start))
              (body-start (n) (let ((c (nth n (agent-shell-ui--group-children
                                               :group-qualified-id "ns-grp"))))
                                (map-elt (agent-shell-ui--nearest-range-matching-property
                                          :property 'agent-shell-ui-section :value 'body
                                          :from (map-elt c :start) :to (map-elt c :end))
                                         :start))))
      ;; Collapse: both member header lines hidden.
      (let ((inhibit-read-only t)) (agent-shell-ui--set-group-collapsed "ns-grp" t))
      (should (get-text-property (member-start 0) 'invisible))
      (should (get-text-property (member-start 1) 'invisible))
      ;; Expand: headers visible; m1 body stays hidden, m2 body visible.
      (let ((inhibit-read-only t)) (agent-shell-ui--set-group-collapsed "ns-grp" nil))
      (should-not (get-text-property (member-start 0) 'invisible))
      (should-not (get-text-property (member-start 1) 'invisible))
      (should (get-text-property (body-start 0) 'invisible))
      (should-not (get-text-property (body-start 1) 'invisible)))))

(ert-deftest agent-shell-ui-group-member-streams-body-stays-nested-test ()
  "A labels-only member that later gains a body stays nested and indented."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "grp" :group-label "T"
      :label-left "run" :label-right "a")
     :navigation 'always)
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "grp" :group-label "T"
      :label-left "run" :label-right "a" :body "streamed body")
     :navigation 'always)
    (should (equal '("ns-m1") (agent-shell-ui-tests--group-child-ids "ns-grp")))
    (let* ((member (car (agent-shell-ui--group-children :group-qualified-id "ns-grp")))
           (body (agent-shell-ui--nearest-range-matching-property
                  :property 'agent-shell-ui-section :value 'body
                  :from (map-elt member :start) :to (map-elt member :end))))
      ;; group indent (2) + body indent (2) = 4.
      (should (equal "    " (get-text-property (map-elt body :start) 'line-prefix))))))

(ert-deftest agent-shell-ui-group-update-existing-member-keeps-group-test ()
  "Updating an existing member never spawns a new group header.
Regression: a caller whose group-id advanced (a message streamed between
a tool call and its completion) must not create an empty group; the
member stays in its original group."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    ;; Member created in group g1.
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "g1" :group-label "T"
      :label-left "… run" :label-right "a")
     :navigation 'always)
    ;; Same member updated, but the caller now passes a *different* group.
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "g2" :group-label "T"
      :label-left "✓ run" :label-right "a" :body "output")
     :navigation 'always)
    ;; No g2 header; the member is still the sole child of g1, indented.
    (should-not (agent-shell-ui--group-header-range "ns-g2"))
    (should (agent-shell-ui--group-header-range "ns-g1"))
    (let ((kids (agent-shell-ui--group-children :group-qualified-id "ns-g1")))
      (should (equal '("ns-m1") (mapcar (lambda (c) (map-elt c :qualified-id)) kids)))
      ;; Body regenerated on update keeps the group+body indent (4).
      (let ((body (agent-shell-ui--nearest-range-matching-property
                   :property 'agent-shell-ui-section :value 'body
                   :from (map-elt (car kids) :start) :to (map-elt (car kids) :end))))
        (should (equal "    " (get-text-property (map-elt body :start) 'line-prefix)))))))

(ert-deftest agent-shell-ui-group-member-added-while-collapsed-stays-hidden-test ()
  "A member added to a folded group is hidden, not popped into view."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "grp" :group-label "T"
      :label-left "run" :label-right "a")
     :navigation 'always)
    (let ((inhibit-read-only t)) (agent-shell-ui--set-group-collapsed "ns-grp" t))
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m2" :group-id "grp" :group-label "T"
      :label-left "run" :label-right "b")
     :navigation 'always)
    (let ((kids (agent-shell-ui--group-children :group-qualified-id "ns-grp")))
      (should (equal '("ns-m1" "ns-m2")
                     (mapcar (lambda (c) (map-elt c :qualified-id)) kids)))
      (dolist (c kids)
        (should (get-text-property (map-elt c :start) 'invisible))))))

(ert-deftest agent-shell-ui-group-collapsed-member-update-stays-hidden-test ()
  "Updating a member in a collapsed group keeps it hidden (no leak).
Regression: a member's in-place edit restored its own visibility while the
separators stayed hidden, collapsing members onto the folded header line."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    ;; Group created collapsed; two labels-only members.
    (dolist (m '("m1" "m2"))
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id "ns" :block-id m :group-id "grp" :group-label "T"
        :group-expanded nil :label-left "… run" :label-right m)
       :navigation 'always))
    ;; Update m1 with a body (as a completion would).
    (agent-shell-ui-update-fragment
     (agent-shell-ui-make-fragment-model
      :namespace-id "ns" :block-id "m1" :group-id "grp" :group-label "T"
      :group-expanded nil :label-left "✓ run" :label-right "m1" :body "output")
     :navigation 'always)
    ;; Every member, including the just-updated one, stays hidden.
    (dolist (c (agent-shell-ui--group-children :group-qualified-id "ns-grp"))
      (should (get-text-property (map-elt c :start) 'invisible))
      ;; A position strictly inside the block is hidden too (not just the
      ;; leading char), so member content can't leak onto the header line.
      (should (get-text-property (1+ (map-elt c :start)) 'invisible)))))

(ert-deftest agent-shell-ui-group-navigation-skips-collapsed-members-test ()
  "Forward navigation steps into visible members but skips folded ones."
  (cl-flet ((walk ()
              (goto-char (point-min))
              (let (visited)
                (while (agent-shell-ui-forward-block)
                  (push (map-elt (get-text-property (point) 'agent-shell-ui-state)
                                 :qualified-id)
                        visited))
                (nreverse visited))))
    (with-temp-buffer
      (agent-shell-ui-mode 1)
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id "d" :block-id "before" :label-left "Before")
       :navigation 'always)
      (dolist (m '("m1" "m2"))
        (agent-shell-ui-update-fragment
         (agent-shell-ui-make-fragment-model
          :namespace-id "d" :block-id m :group-id "g" :group-label "G"
          :label-left "run" :label-right m :body "b")
         :navigation 'always))
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id "d" :block-id "after" :label-left "After")
       :navigation 'always)
      ;; Expanded: header and both members are visited.
      (should (equal '("d-before" "d-g" "d-m1" "d-m2" "d-after") (walk)))
      ;; Collapsed: members are skipped.
      (let ((inhibit-read-only t)) (agent-shell-ui--set-group-collapsed "d-g" t))
      (should (equal '("d-before" "d-g" "d-after") (walk))))))

(ert-deftest agent-shell-ui-group-delete-member-keeps-header-test ()
  "Deleting a member leaves the header and the remaining members intact."
  (with-temp-buffer
    (agent-shell-ui-mode 1)
    (dolist (id '("m1" "m2"))
      (agent-shell-ui-update-fragment
       (agent-shell-ui-make-fragment-model
        :namespace-id "ns" :block-id id :group-id "grp" :group-label "T"
        :label-left "run" :label-right id)
       :navigation 'always))
    (agent-shell-ui-delete-fragment :namespace-id "ns" :block-id "m1")
    (should (agent-shell-ui--group-header-range "ns-grp"))
    (should (equal '("ns-m2") (agent-shell-ui-tests--group-child-ids "ns-grp")))))

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
