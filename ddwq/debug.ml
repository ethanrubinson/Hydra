let debug_mode_active = true

type debug_type = INFO | WARN | ERROR | FATAL | NONE

let debug t string_to_print = (let t_String = "\027[0m" ^ (match t with |INFO -> "\027[34m[INFO] >> " |WARN -> "\027[33m[WARN] >> "|ERROR -> "\027[31m[ERROR] >> "| FATAL -> "\027[31m\027[5m[FATAL] >> "|NONE -> "\027[32m") in if debug_mode_active || t = NONE then (print_endline (t_String ^ string_to_print ^ "\027[0m" )) else ())

let rec print_int_list = function
	| [] -> ()
	| h::t -> (debug NONE (string_of_int h)); print_int_list t