open Async.Std

let () = Command.async_basic
    ~summary:"Slave node!"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> anon ("port" %: int)
    )
    (fun port () -> Slave.init port)
         |> Command.run

