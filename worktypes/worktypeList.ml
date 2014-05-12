open Work_full
open Work_addi
open Work_exec

let list_worktypes () =
  let types = String.concat "\n  - " (Ddwq.list_worktypes ()) in
  if types = "" then "No worktypes installed!" else "Installed worktypes:\n\n  - "^ types

