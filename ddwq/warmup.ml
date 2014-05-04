open Async.Std

let fork d f1 f2 = ignore( d >>= fun v -> (f1 v); (f2 v); return ())


let deferred_map l f =
  let rec map_loop = function
    | [] ->  return []
    | h_def::tl_def -> h_def >>= fun h ->
					map_loop tl_def >>= fun tl ->
					return (h::tl)
  in
   map_loop (List.map (fun x -> f x) l)