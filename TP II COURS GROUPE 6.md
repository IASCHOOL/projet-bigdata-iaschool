```python
spark
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[1]: </div>




    <div>
        <p><b>SparkSession - hive</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="/?o=2829625158618277#setting/sparkui/0607-015622-goo145/driver-2632736371343267054">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.1.1</code></dd>
      <dt>Master</dt>
        <dd><code>local[8]</code></dd>
      <dt>AppName</dt>
        <dd><code>Databricks Shell</code></dd>
    </dl>
</div>

    </div>




```python
type(spark)
spark.conf.set()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">Out[2]: pyspark.sql.session.SparkSession</div>



```python
## La commande suivante permet de configurer spark pour se connecter à un azure blob storage particulier 
spark.conf.set("fs.azure.account.key.iasblob1.blob.core.windows.net",
  "Jx3jsLRNREeC4Gc3MX0YeW6qr0j7oxWRer9G/Mfh7ZQEgLhTdOqArojfkacpyglH/C0iHPXFidm9JXgZTywGuQ==")
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
#Récupérer les fichiers

df = spark.read.csv("wasbs://big-data@iasblob1.blob.core.windows.net/houses.csv", header=True, inferSchema=True)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"><span class="ansi-red-fg">---------------------------------------------------------------------------</span>
<span class="ansi-red-fg">AnalysisException</span>                         Traceback (most recent call last)
<span class="ansi-green-fg">&lt;command-863569460926966&gt;</span> in <span class="ansi-cyan-fg">&lt;module&gt;</span>
<span class="ansi-green-intense-fg ansi-bold">      1</span> <span class="ansi-red-fg">#Récupérer les fichiers</span>
<span class="ansi-green-intense-fg ansi-bold">      2</span> 
<span class="ansi-green-fg">----&gt; 3</span><span class="ansi-red-fg"> </span>df <span class="ansi-blue-fg">=</span> spark<span class="ansi-blue-fg">.</span>read<span class="ansi-blue-fg">.</span>csv<span class="ansi-blue-fg">(</span><span class="ansi-blue-fg">&#34;wasbs://big-data@iasblob1.blob.core.windows.net/houses.csv&#34;</span><span class="ansi-blue-fg">,</span> header<span class="ansi-blue-fg">=</span><span class="ansi-green-fg">True</span><span class="ansi-blue-fg">,</span> inferSchema<span class="ansi-blue-fg">=</span><span class="ansi-green-fg">True</span><span class="ansi-blue-fg">)</span>

<span class="ansi-green-fg">/databricks/spark/python/pyspark/sql/readwriter.py</span> in <span class="ansi-cyan-fg">csv</span><span class="ansi-blue-fg">(self, path, schema, sep, encoding, quote, escape, comment, header, inferSchema, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace, nullValue, nanValue, positiveInf, negativeInf, dateFormat, timestampFormat, maxColumns, maxCharsPerColumn, maxMalformedLogPerPartition, mode, columnNameOfCorruptRecord, multiLine, charToEscapeQuoteEscaping, samplingRatio, enforceSchema, emptyValue, locale, lineSep, pathGlobFilter, recursiveFileLookup, modifiedBefore, modifiedAfter, unescapedQuoteHandling)</span>
<span class="ansi-green-intense-fg ansi-bold">    762</span>             path <span class="ansi-blue-fg">=</span> <span class="ansi-blue-fg">[</span>path<span class="ansi-blue-fg">]</span>
<span class="ansi-green-intense-fg ansi-bold">    763</span>         <span class="ansi-green-fg">if</span> type<span class="ansi-blue-fg">(</span>path<span class="ansi-blue-fg">)</span> <span class="ansi-blue-fg">==</span> list<span class="ansi-blue-fg">:</span>
<span class="ansi-green-fg">--&gt; 764</span><span class="ansi-red-fg">             </span><span class="ansi-green-fg">return</span> self<span class="ansi-blue-fg">.</span>_df<span class="ansi-blue-fg">(</span>self<span class="ansi-blue-fg">.</span>_jreader<span class="ansi-blue-fg">.</span>csv<span class="ansi-blue-fg">(</span>self<span class="ansi-blue-fg">.</span>_spark<span class="ansi-blue-fg">.</span>_sc<span class="ansi-blue-fg">.</span>_jvm<span class="ansi-blue-fg">.</span>PythonUtils<span class="ansi-blue-fg">.</span>toSeq<span class="ansi-blue-fg">(</span>path<span class="ansi-blue-fg">)</span><span class="ansi-blue-fg">)</span><span class="ansi-blue-fg">)</span>
<span class="ansi-green-intense-fg ansi-bold">    765</span>         <span class="ansi-green-fg">elif</span> isinstance<span class="ansi-blue-fg">(</span>path<span class="ansi-blue-fg">,</span> RDD<span class="ansi-blue-fg">)</span><span class="ansi-blue-fg">:</span>
<span class="ansi-green-intense-fg ansi-bold">    766</span>             <span class="ansi-green-fg">def</span> func<span class="ansi-blue-fg">(</span>iterator<span class="ansi-blue-fg">)</span><span class="ansi-blue-fg">:</span>

<span class="ansi-green-fg">/databricks/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py</span> in <span class="ansi-cyan-fg">__call__</span><span class="ansi-blue-fg">(self, *args)</span>
<span class="ansi-green-intense-fg ansi-bold">   1302</span> 
<span class="ansi-green-intense-fg ansi-bold">   1303</span>         answer <span class="ansi-blue-fg">=</span> self<span class="ansi-blue-fg">.</span>gateway_client<span class="ansi-blue-fg">.</span>send_command<span class="ansi-blue-fg">(</span>command<span class="ansi-blue-fg">)</span>
<span class="ansi-green-fg">-&gt; 1304</span><span class="ansi-red-fg">         return_value = get_return_value(
</span><span class="ansi-green-intense-fg ansi-bold">   1305</span>             answer, self.gateway_client, self.target_id, self.name)
<span class="ansi-green-intense-fg ansi-bold">   1306</span> 

<span class="ansi-green-fg">/databricks/spark/python/pyspark/sql/utils.py</span> in <span class="ansi-cyan-fg">deco</span><span class="ansi-blue-fg">(*a, **kw)</span>
<span class="ansi-green-intense-fg ansi-bold">    114</span>                 <span class="ansi-red-fg"># Hide where the exception came from that shows a non-Pythonic</span>
<span class="ansi-green-intense-fg ansi-bold">    115</span>                 <span class="ansi-red-fg"># JVM exception message.</span>
<span class="ansi-green-fg">--&gt; 116</span><span class="ansi-red-fg">                 </span><span class="ansi-green-fg">raise</span> converted <span class="ansi-green-fg">from</span> <span class="ansi-green-fg">None</span>
<span class="ansi-green-intense-fg ansi-bold">    117</span>             <span class="ansi-green-fg">else</span><span class="ansi-blue-fg">:</span>
<span class="ansi-green-intense-fg ansi-bold">    118</span>                 <span class="ansi-green-fg">raise</span>

<span class="ansi-red-fg">AnalysisException</span>: Path does not exist: wasbs://big-data@iasblob1.blob.core.windows.net/houses.csv</div>



```python
## Affiche le dataframe
df.show() ####
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"><span class="ansi-red-fg">---------------------------------------------------------------------------</span>
<span class="ansi-red-fg">NameError</span>                                 Traceback (most recent call last)
<span class="ansi-green-fg">&lt;command-863569460926967&gt;</span> in <span class="ansi-cyan-fg">&lt;module&gt;</span>
<span class="ansi-green-fg">----&gt; 1</span><span class="ansi-red-fg"> </span>df<span class="ansi-blue-fg">.</span>show<span class="ansi-blue-fg">(</span><span class="ansi-blue-fg">)</span> <span class="ansi-red-fg">####</span>

<span class="ansi-red-fg">NameError</span>: name &#39;df&#39; is not defined</div>



```python
## 3. Calculer le prix moyen, le prix minimal et maximum des maisons
from pyspark.sql import functions as F
moyenne = df.agg(F.avg('price').alias("Moyenne")).show()
max_price = df.agg(F.max('price').alias('Max')).show()
min_price = df.agg(F.min('price').alias('Minimum')).show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+-----------------+
          Moyenne|
+-----------------+
267154.9114363987|
+-----------------+

+----------------+
             Max|
+----------------+
377897.064623253|
+----------------+

+-------------------+
            Minimum|
+-------------------+
-19196.814585797973|
+-------------------+

</div>



```python
## Calcul le prix moyen, le prix minimum et maximum des maisons 

stats = df.agg(F.avg('price').alias("Moyenne"),F.max('price').alias('Max'),F.min('price').alias('Minimum'))
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
stats.show() ### df Stats
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+-----------------+----------------+-------------------+
          Moyenne|             Max|            Minimum|
+-----------------+----------------+-------------------+
267154.9114363987|377897.064623253|-19196.814585797973|
+-----------------+----------------+-------------------+

</div>



```python
## 4 Staocker le dataFrame stats dans le blob storage/
sts = stats.coalesce(1).write.format('com.databricks.spark.csv').save('/FileStore/tables/stats/stats.csv',header = True)

```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
stats1 = spark.read.csv('/FileStore/tables/stats/stats.csv', header=True, inferSchema=True)
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout"></div>



```python
stats1.show()
```


<style scoped>
  .ansiout {
    display: block;
    unicode-bidi: embed;
    white-space: pre-wrap;
    word-wrap: break-word;
    word-break: break-all;
    font-family: "Source Code Pro", "Menlo", monospace;;
    font-size: 13px;
    color: #555;
    margin-left: 4px;
    line-height: 19px;
  }
</style>
<div class="ansiout">+-----------------+----------------+-------------------+
          Moyenne|             Max|            Minimum|
+-----------------+----------------+-------------------+
267154.9114363987|377897.064623253|-19196.814585797973|
+-----------------+----------------+-------------------+

</div>



```python
## Fait en machine locale 

#Importation des modules de bases
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import numpy as np 
import pandas

print("#######################################################")
print("# \t Réponses aux questions du TP2\t #")
print("#######################################################")
#Vecteur aléatoire
print(" 1) Variable SparkContext")

print("#######################################################")
print("a) -----Vecteur aléatpoire contenant 100 000 valeurs aléatoire-------")
## Nous générons de nombres aléatoires de de l'intervalles [-4, 5[
vecteur  = 5 * np.random.random_sample(100000)-4 ## [-4, 5]
print("vecteur= 5 * np.random.random_sample(100000)-4 ")
print("#\t\t\t\t\t\ Fait \t\t\t\t\t #")
print("########################################################")
print("b) ----------Créer une variable SparkContext---------\n")
sc = SparkContext()
spark= SparkSession(sc)
#sqlContext = SQLContext(sc)
print("#######################################################")
print("c) ----------Parallelizer le vecteur aléatoire----------\n")
## paralellizer
rdd = sc.parallelize(vecteur)

print("#######################################################")
print("d) ----------Filtrer et garder les valeurs positives---------\n")
## recueprer les valeur
rdd_filter = rdd.filter(lambda x: x>0)
print("Affichage des 100 premiers nombres")
print(rdd_filter.take(100))
print('d) - Fait')

print("#######################################################")
### affichage ..

print("e)---------- Calculer la somme des valeurs--------\n")
## calculer la somme de toutes les valeurs
somme = rdd.reduce(lambda x, y: x+y)
## afficher le resultat
print("somme = rdd.reduce(lambda x, y: x+y)")
print("Somme: ",somme)

print("#######################################################")
## ecrire dans un csv
print("f------ Afficher le résultat et l’écrire dans un fichier csv--------")

#rdd_filter.coalesce(1).saveAsTextFile("/Users/amadou/COURS_IA_SCHOOL_2021/Spark/valeurs_positives.csv")
rdd2 = rdd_filter.map(lambda x: float(x))
DF = rdd2.toDF(FloatType())


## sauvegarder le résultat dans un csv 
DF.coalesce(1).write.format("csv").save("Data_Exercice_2")
print("Chemin du dossier: Data_Exercice_2")
##DataFrame du résulat de la somme
df = pandas.DataFrame([somme], columns= ['Somme'])
print(df.head())
## Sauvegarder dans un fichier  au format csv
df.to_csv('resultat.csv')

print("Enregisté sur (Data_Exercice/resultat.csv)")

```
