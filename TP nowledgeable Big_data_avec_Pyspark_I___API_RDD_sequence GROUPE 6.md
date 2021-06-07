```python
from IPython.display import HTML
```


```python
rdd2= rdd.flatMap(lambda st: st.split(' '))

```


```python
rdd_min= rdd.min()
rdd_max = rdd.max()
```


```python
rdd_std= rdd.stdev()
```


```python

average = rdd.reduce(lambda x,y: x+y)/rdd.count()
```


```python
rdd2 = rdd.filter(lambda x: x >= 0)
```


```python
len_rdd = rdd.map(lambda x:len(x))
```


```python
rdd2  = rdd.map(lambda x:x*2)
```


```python
                        
                    Il s'agit de la réplication des données; partition de la donnée ,la garantie de la haute disponibilité et la tolérance à la panne
```
