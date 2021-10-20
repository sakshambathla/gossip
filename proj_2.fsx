#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
printfn("hello from remote1")

open System
open System.Security.Cryptography
open System.Text
open Akka.Actor
open Akka.FSharp
open System.Diagnostics
open Akka.Configuration

type InitActorMsg = double*double*bool*List<IActorRef>*int
type InitSupervisorMessage = string*string*int
type FirstContact = string*int*List<IActorRef>
type GossipMessage = string
type ConvergenceMessage = string*int
type GossipPropogationMessage = string*string
type PushSumPropogationMessage = int*string
type PushSumInputMessage = double*double

let system = ActorSystem.Create("ClientFsharp", Configuration.defaultConfig())

let args : string array = fsi.CommandLineArgs |>  Array.tail
let mutable numNodes = args.[0] |> int
let topology = args.[1]
let algorithmName = args.[2]
let mutable processingDone = false
printfn "args: %A" args
let random = System.Random()
if topology = "3d" || topology = "imp3d" then
    let cubeLength = ceil(Math.Cbrt(numNodes |> float))
    numNodes <- ((cubeLength**3.0) |> int)
    printfn "Number of nodes %d" numNodes

type Worker(name) =
    inherit Actor()
    let mutable s: float = 0.0
    let mutable w: float = 0.0
    let mutable convergenceCount = 0
    let mutable id = 0
    let maxGossip = 10
    let mutable randomized = false
    let mutable supervisor = null
    let mutable myActors: list<IActorRef> = []
    let mutable converged = false
    let mutable currRatio: double = 0.0
    let mutable delta = 0.000001
    override this.OnReceive message = 
        match message with
        | :? InitActorMsg as message ->
            let (sum, weight, rand, actorlist, index) = unbox<InitActorMsg> message
            s <- sum
            w <- weight
            myActors <- actorlist
            randomized <- rand
            supervisor <- this.Sender
            id <- index
        | :? GossipMessage as message ->
            let (gossipString) = unbox<GossipMessage> message
            if convergenceCount >= maxGossip then
                ignore ()
            else
                convergenceCount <- convergenceCount + 1
                if convergenceCount >= maxGossip then
                    supervisor <! ("converged", id)
                let chosenNeighbour = random.Next(0,myActors.Length)
                myActors.[chosenNeighbour] <! message
        | :? PushSumInputMessage as message ->
            let (nS, nW) = unbox<PushSumInputMessage> message
            if not converged then
                s <- s + nS
                w <- w + nW
            else 
                this.Sender <! message    
        | :? PushSumPropogationMessage as message ->
            let (newId, gossipString) = unbox<PushSumPropogationMessage> message
            let mutable ratio: double = double (s / w)
            if not converged then
                let diff: double = abs(double(currRatio) - double(ratio))
                if diff <= delta then
                    convergenceCount <- convergenceCount + 1
                    if convergenceCount >= 3 then
                        converged <- true
                        supervisor <! ("converged", id)
                else
                    convergenceCount <- 0
                currRatio <- ratio
                let chosenNeighbour = random.Next(0,myActors.Length)
                myActors.[chosenNeighbour] <! (double(s * 0.5), double(w * 0.5))
                s <- double(s * 0.5)
                w <- double(w * 0.5) 

        | _ -> failwith "invalid message passed to the worker"


let GetWorkers = fun (num: int) ->
    let mutable myActors = []
    for i in 1..num do
        myActors <- List.append myActors [system.ActorOf(Props(typedefof<Worker>, [| string(id) :> obj |]))]
    myActors

let GetLinedNeighboursArray = fun (i: int) (actors: list<IActorRef>) ->
    let mutable currentNeighbours = []
    if i > 0 then
        currentNeighbours <- List.append currentNeighbours [actors.[i-1]]
    if i < actors.Length-1 then   
        currentNeighbours <- List.append currentNeighbours [actors.[i+1]]
    currentNeighbours

let GetAllNeighboursArray = fun (i: int) (actors: list<IActorRef>) ->
    let mutable currentNeighbours = []
    for j in 0..actors.Length-1 do
        if j<>i then
            currentNeighbours <- List.append currentNeighbours [actors.[j]]
    currentNeighbours

let Get3dNeighboursArray = fun (i: int) (actors: list<IActorRef>) ->
    let mutable currentNeighbours = []
    let numNodes = actors.Length
    let edge = ceil(Math.Cbrt(numNodes |> float)) |> int
    let sqedge = edge * edge
    let neighbours = [|i+1; i-1; i + edge; i-edge; i+sqedge; i-sqedge|]
    let mutable n = 0
    
    for j in neighbours do
        if n < 4 then
            if (j >= 0 && j < numNodes && j%sqedge = i%sqedge) then
                currentNeighbours <- List.append currentNeighbours [actors.[j]]
        else
            if (j >= 0 && j < numNodes) then
                currentNeighbours <- List.append currentNeighbours [actors.[j]]        
        n <- n+1
    currentNeighbours

let GetImp3dNeighboursArray = fun (i: int) (actors: list<IActorRef>) ->
    let mutable currentNeighbours = []
    let numNodes = actors.Length
    let edge = ceil(Math.Cbrt(numNodes |> float)) |> int
    let sqedge = edge * edge
    let neighbours = [|i+1; i-1; i + edge; i-edge; i+sqedge; i-sqedge|]
    let mutable n = 0
    for j in neighbours do
        if n < 4 then
            if (j >= 0 && j < numNodes && j%sqedge = i%sqedge) then
                currentNeighbours <- List.append currentNeighbours [actors.[j]]
        else
            if (j >= 0 && j < numNodes) then
                currentNeighbours <- List.append currentNeighbours [actors.[j]]        
        n <- n+1
    let randomNeighbour = random.Next(0,numNodes)
    currentNeighbours <- List.append currentNeighbours [actors.[randomNeighbour]]
    currentNeighbours
                
let mutable allWorkers = GetWorkers(numNodes)

type Supervisor(name) =
    inherit Actor()
    let mutable convergence = 0
    override this.OnReceive message =
        match message with
            | :? InitSupervisorMessage as message -> 
                let (topo, algo, numberNodes) = unbox<InitSupervisorMessage> message
                printfn "got the input topology: %s, algorithm: %s, numberNodes %d" topo algo numberNodes
                
                for i in 0..numberNodes-1 do
                    let mutable currentNeighbours = []
                    match topology with
                    | "full" ->
                        currentNeighbours <- GetAllNeighboursArray i allWorkers
                    | "3d" ->
                        currentNeighbours <- Get3dNeighboursArray i allWorkers
                    | "line" ->
                        currentNeighbours <- GetLinedNeighboursArray i allWorkers
                    | "imp3d" ->
                        currentNeighbours <- GetImp3dNeighboursArray i allWorkers
                    | _ -> failwith "invalid topology"

                    allWorkers.[i] <! (i |> double ,1.0,false,currentNeighbours, i)
                for i in 0..allWorkers.Length-1 do
                    if algorithmName = "gossip" then
                        allWorkers.[i] <! ("lets gossip")
                    else if algorithmName = "push-sum" then    
                        allWorkers.[i] <! (i,"lets gossip")
                    else 
                        failwith "invalidAlgorithm"         
            | :? ConvergenceMessage as message ->
                let (mesg, actorName) = unbox<ConvergenceMessage> message
                convergence <- convergence + 1
                //printfn "%d actor number %s. Total Convergence: %d" actorName mesg convergence
                if convergence%100 = 0 then
                    printfn "Total Convergence: %d" convergence
                if convergence >= numNodes then
                    printfn "All actors converged"
                    processingDone <- true
            | :?  GossipPropogationMessage as message-> 
                let (mesg, str) = unbox<GossipPropogationMessage> message
                for i in 0..allWorkers.Length-1 do
                    allWorkers.[i] <! ("lets gossip")
            | :?  PushSumPropogationMessage as message-> 
                let (mesg, str) = unbox<PushSumPropogationMessage> message
                for i in 0..allWorkers.Length-1 do
                    allWorkers.[i] <! (i,"lets gossip")                
            | _ -> failwith "invalid message passed to the supervisor"



let mySupervisor = system.ActorOf(Props(typedefof<Supervisor>, [| string(1) :> obj |]))

let clock = Stopwatch.StartNew()
let mutable lastTimestamp  = clock.ElapsedMilliseconds
mySupervisor <! (topology, "other", numNodes)

while not processingDone do
    ignore ()
    if (clock.ElapsedMilliseconds - lastTimestamp) >= int64(300) then
         lastTimestamp <- clock.ElapsedMilliseconds
         if algorithmName = "gossip" then
            mySupervisor <! ("Gossiping", "Fire!")
         else
            mySupervisor <! (1, "Fire!")

clock.Stop()
printfn "Time taken for Convergence : %A Milliseconds" clock.ElapsedMilliseconds