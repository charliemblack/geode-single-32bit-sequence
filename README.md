## 32 Bit Sequence Number Generator

Sometimes it is necessary to generate keys in a linear fashion.   This sample project show how this can be done with Geode.

To generate the sequence we use Geode onRegion function calls to route the request to the correct server that is holding the sequence.  We also use Geode Distributed Locks to protect the sequence from concurrent modification.   For durability of the sequence we store the current value in a Geode partitioned region with a redundant copy stored on another host.

The sequence number generator returns a block of numbers to offset the cost of the remote procedure call.   If the application architect wishes to retrieve one sequence number at time they can.

## How to use from a client perspective:

```
// Create a sequence object which is a thin wrapper to a Geode function call
// All state with respect to the sequence is held durable by Geode
Unsigned32bitSequence unsigned32bitSequence = new Unsigned32bitSequence("mySequence");

...

// Retrieve a block of sequence numbers for the application to use.
long[] result = unsigned32bitSequence.getNextBlock(blockSize);
```

## How to Run Provided Demo

From the project directory build the project.
```
./gradlew clean
./gradlew installDist
```

Then change in to the <project home>/scripts directory.
```
cd scripts
```

Start the Geode cluster and deploy the 32 bit sequence generator.
```
./startGeode
```
Once Geode is running open a new window or change directory into the client foleder that will test the seqence number generator.
```
cd build/install/geode-single-32bit-sequence/bin
```
Run the client with the command you would like to try, here is an example where I run 20 threads retrieving 1000 sequence numbers with 500 iterations.
```
./geode-single-32bit-sequence --threads 20 --block-size 1000 --iterations 500
```

If everthing went right the output should look something like this:
```
Checking order of 10000000 items - right count true
Everything OK
```
If there was something wrong we would see the output from this System.out:
```
System.out.println("Failure : " + test[i - 1] + " > " + test[i]);
```
Feel free to kill a Geode server while the test is running and see how the clients react.   Geode will automatically detect the down server and retry on another server.

Then to shut down the Geode processes a helper script is provided:

```
cd <project home>/scripts
./shutdowmGeode
```

If there are any problem let me know and we can figure out where the problem is.
