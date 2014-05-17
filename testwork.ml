let square x = x * x

let minisleep (sec: float) =
    ignore (Unix.select [] [] [] sec)

let rec fact x =
    if x <= 1 then 1 else x * fact (x - 1)

let () = (minisleep 2.0); print_endline (string_of_int (fact 4))