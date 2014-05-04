(** Async warmup exercises *)

open Async.Std

(** [fork d f1 f2] runs the [f1] and [f2] once [d] becomes determined.  The
    output of [f1] and [f2] should be ignored. *)
val fork : 'a Deferred.t -> ('a -> 'b Deferred.t)
                         -> ('a -> 'c Deferred.t) -> unit

(** This function has the same specification as {! Deferred.List.map}.
    [deferred_map l f] applies [f] in parallel to each element of [l].  When
    all of the calls to [f] are complete, [deferred_map] returns a list
    containing the results. *)
val deferred_map : 'a list -> ('a -> 'b Deferred.t) -> 'b list Deferred.t

