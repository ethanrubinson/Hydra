open Async.Std

let () = Command.async_basic
    ~summary:"I'm a fucking SALVE!"
    ~readme:WorktypeList.list_worktypes
    Command.Spec.(
      empty
      +> anon ("port" %: int)
    )
    (fun port () -> Slave.init port)
  |> Command.run

