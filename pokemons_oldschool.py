
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df_pk = spark.table("work_dataeng.pokemon_tatielle_ext");

df_pk.show();


df_gene = spark.table("work_dataeng.generation_tatielle")
df_gene.show();

df_gene = df_gene.filter(df_gene["date_introduced"] < "2002-11-21")
df_gene = df_gene.cache
df_gen_pok = df_pk.join(df_gene, 'generation', how='inner')

df_gen_pok.write.mode('overwrite').format('orc') \
        .saveAsTable('work_dataeng.pokemons_oldschool_tatielle')


     

