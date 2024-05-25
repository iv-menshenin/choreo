# choreo

Choreographic interaction over UDP

## What is this thing

By interacting through the UDP protocol over a broadcast channel, your application can discover other instances in order to coordinate the execution of a common process.

## What it can do

Currently, two interesting techniques have been implemented:
 1. Determination of the addresses of all available instances of the application.
 2. Choreographic distribution of keys between shards (not based on hashing, but on negotiation).