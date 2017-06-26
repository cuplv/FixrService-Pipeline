# Combinator Library for Incremental Processing Pipelines

## Quick start

Check out the example in SimpleExample.scala . 

'''
{ ((m0 :--PlusOne--> m2) <-*BatchProduct.composer[IDInt,IDInt]*-> (m1 :--PlusOne--> m3)) :--TimesPair--> m4 } :< {
       (PlusOne--> m5) ~ (Plus(10)--> m6 :--Plus(100)--> m7 :--Plus(-12)--> m8)
}
'''

