Myria setup for Bill's PNNL demo 2013-09-24

### One-time initialization

```
git submodule init
git submodule update

cd myria
gradle assemble
```

### Starting the cluster

```
cd myria/myriadeploy

./stop_all_by_force.py ../../billdemo.cfg
./start_master.py ../../billdemo.cfg
./start_workers.py ../../billdemo.cfg
```
