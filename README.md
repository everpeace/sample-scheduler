# sample scheduler

## prerequisites

- go 1.25+

## How to run

```shell
# create multi-node kubernetes cluster
minikube start --nodes 3 -p sample-scheduler-test

# run the scheduler 
go run main.go
```

Please run belows on the different terminal:

### Single Pod Case

```shell
kubectl apply -f example/single-pod.yaml
```

you can see the log in secandary scheduler:

```
I0121 12:40:28.169046   99025 scheduler.go:88] "binding workload to nodes" workload="single-pod" nodes=["sample-scheduler-test-m02"]
```

### PodGroup case

NOTE: this is not implemented yet.

The example pod group size 2.

```shell
kubectl apply -f example/pod-group-pod1.yaml
```

Expected scenario:
- pod-group-pod1 is pending, scheduler shows the pod group has not be reached to its size
- `kubectly apply -f example/pod-group-pod2.yaml`
- then scheduler detects `pod-group` is ready for schedule, and bind two pods to available nodes at once.

### Preemption case

NOTE: this is not implemented yet.

### TODOs

There are bunch of tasks todo. I needed more time to complete the tasks

- Make schecheduler state be more sophisiticated
- Complete event handlers
- Implement preemption logic
- etc.
