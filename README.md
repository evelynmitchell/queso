Queso

This project is to create a hello world version of a distributed consensus system on the quepaxa algorithm. Cloudflare has implemented it in a project called meerkat.

There are other distributed concensus algorithms, Paxos and Raft, for example. And there are other distributed concensus implementations, etcd in Kubernetes.

I'd like to go through creating from the ground up, a test harness, a model of desired properties and non-desired properties, and an implementation which gradually provides a robust, validated, verified and performant concensus system.

The goals of a concensus system are to provide a consistent data plane, which can be used to provide the same 'view of reality' when the parts of the system which make up the whole are unable to be relied on to be available to provide a statement of their state on request, because one or both ends or the connection may be unreliable.

Concensus allows for failure to be a normal state of the world.

A good concensus implementation can continue to fully function even though some or a majority of the components are unavailable.

The use case is normally in machine to machine systems, and below the layer of UI/UX. It provides a property, consistentcy. This is important for being able to rely on other properties of a system, such as data integrity or compute availability, or visibility.

Please read the documents, write up a white paper backgrounder describing this as a problem space, with references, and outline a list of properties that must hold, that are desireable, and a testing plan.

Ask any questions you may have.

# References

https://blog.cloudflare.com/meerkat-introduction/

https://bford.info/pub/os/quepaxa/quepaxa.pdf
