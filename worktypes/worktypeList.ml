open Work_full

let list_worktypes () =
  let types = String.concat "\n  - " (Ddwq.list_worktypes ()) in
  if types = "" then "No worktypes installed!" else "Installed worktypes:\n\n  - "^ types

