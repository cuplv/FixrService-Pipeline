# Combinator Library for Incremental Processing Pipelines

## Quick start

Check out the example in SimpleExample.scala . 

```
{ ((m0 :--PlusOne--> m2) <-*BatchProduct.composer[IDInt,IDInt]*-> (m1 :--PlusOne--> m3)) :--TimesPair--> m4 } :< {
       (PlusOne--> m5) ~ (Plus(10)--> m6 :--Plus(100)--> m7 :--Plus(-12)--> m8)
}
```

encodes the following:

<img src="pipeline-example.png" width="1000" height="305">

Here's how the language is defined (adopting Scala's typing notation):

```
DataMap  Dm[D]    Transformer  Tr[I,O]    Composer   Cr[L,R]

Pipe        P[D]     ::=  Dm[D]                                        -- DataNode[D]
                      |   P[I]  :--Tr[I,O]-->  Dm[O]  D == O           -- TransformationPipe[I,O]
                      |   P[L]  <-*Cr[L,R]*->  P[R]   D == (L,R)       -- CompositionPipe[I,O]
                      |   P[I]  :<  l[I,O]            D == O           -- JunctionPipe[I,O]
                      |   P[L]  ||  P[R]              D == Either L R  -- ParallelPipe[L,R]

Partial Pipe   l[I,D]   ::=   :--Tr[I,O]-->  Dm[O]    D == O           -- PartialTransformationPipe[I,O]
                         |    <-*Cr[L,R]*->  P[R]     D == (L,R)       -- PartialCompositionPipe[L,R]
                         |    l[I,O]  l[O,D]                           -- PartialHeadPipe[I,O,D]
                         |    l[I,L] ~ l[I,R]         D == Either L R  -- PartialParallelPipes[I,L,R]
```

