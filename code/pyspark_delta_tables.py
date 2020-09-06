# import
from pyspark.sql.window import Window
from pyspark.sql.functions import *
spark.catalog.clearCache()


# Base de dados de Usuários / Alunos.
path_usuario = 'dbfs:/FileStore/tables/usuario_nome_nascimento_formacao.csv'

df_usuario = spark.read.format('csv')\
                       .option('inferSchema', False)\
                       .option('header', True)\
                       .option("encoding", "ISO-8859-1")\
                       .option('sep', ',')\
                       .load(path_usuario)

df_usuario = df_usuario.withColumnRenamed("Nome", "primeiro_nome")
df_usuario = df_usuario.withColumn("ano_nascimento", df_usuario.ano_nascimento.cast('int'))
df_usuario.cache().count()

spark.sql("DROP TABLE IF EXISTS dmd_usuario")
df_usuario.write.format("delta").saveAsTable('dmd_usuario')


# Base de dados de Diplomas / Cursos.
path_curso = 'dbfs:/FileStore/tables/nome_curso_termino_carga.csv'

df_curso = spark.read.format('csv')\
                     .option('inferSchema', False)\
                     .option('header', True)\
                     .option("encoding", "ISO-8859-1")\
                     .option('sep', ',')\
                     .load(path_curso)

df_curso = df_curso.withColumn('data', from_unixtime(unix_timestamp(trim(df_curso.data).substr(1, 10), 'dd/MM/yyyy')).cast('date'))
df_curso = df_curso.withColumnRenamed("Nome_usuario", "primeiro_nome")
df_curso = df_curso.withColumnRenamed("nome_evento", "curso")
df_curso = df_curso.withColumnRenamed("carga", "carga_horaria")
df_curso.cache().count()

spark.sql("DROP TABLE IF EXISTS dmd_curso")
df_curso.write.format("delta").saveAsTable('dmd_curso')


# Base de dados de Avaliações.
path_nota = 'dbfs:/FileStore/tables/curso_nota.csv'

df_nota = spark.read.format('csv')\
                     .option('inferSchema', False)\
                     .option('header', True)\
                     .option("encoding", "UTF-8")\
                     .option('sep', ';')\
                     .load(path_nota)

df_nota = df_nota.withColumn('data', from_unixtime(unix_timestamp(trim(df_nota.data).substr(1, 10), 'dd-MM-yyyy')).cast('date'))
df_nota = df_nota.withColumn("avaliacao", df_nota.avaliacao.cast('int'))
df_nota = df_nota.withColumnRenamed("estudante", "primeiro_nome")
df_nota = df_nota.withColumnRenamed("data", "data_matricula")
df_nota = df_nota.withColumnRenamed("avaliacao", "nota")
df_nota = df_nota.drop('_c5')
df_nota.cache().count()

spark.sql("DROP TABLE IF EXISTS dmd_nota")
df_nota.write.format("delta").saveAsTable('dmd_nota')


# Base de dados de Matrículas e Aulas Assistidas.
path_alunos = 'dbfs:/FileStore/tables/alunos_cursos.csv'

df_alunos = spark.read.format('csv')\
                     .option('inferSchema', False)\
                     .option('header', True)\
                     .option("encoding", "UTF-8")\
                     .option('sep', ';')\
                     .load(path_alunos)

df_alunos = df_alunos.withColumn('Da-ed-Star', from_unixtime(unix_timestamp(trim(df_alunos['Da-ed-Star']).substr(1, 10), 'dd-MM-yyyy')).cast('date'))
df_alunos = df_alunos.withColumn('it-Vi-Last', from_unixtime(unix_timestamp(trim(df_alunos['it-Vi-Last']).substr(1, 10), 'dd-MM-yyyy')).cast('date'))
df_alunos = df_alunos.withColumn("Progress%", regexp_replace(df_alunos['Progress%'],'%','').cast('int'))
df_alunos = df_alunos.withColumn("Questions Asked", df_alunos['Questions Asked'].cast('int'))
df_alunos = df_alunos.withColumn("Questions Answered", df_alunos['Questions Answered'].cast('int'))
df_alunos = df_alunos.withColumnRenamed("Super Academia Data Science", "curso")
df_alunos = df_alunos.withColumnRenamed("Student", "primeiro_nome")
df_alunos = df_alunos.withColumnRenamed("Da-ed-Star", "data_inicio")
df_alunos = df_alunos.withColumnRenamed("it-Vi-Last", "data_ultimo_acesso")
df_alunos = df_alunos.withColumnRenamed("Progress%", "progresso")
df_alunos = df_alunos.withColumnRenamed("Questions Asked", "perguntas_feitas")
df_alunos = df_alunos.withColumnRenamed("Questions Answered", "perguntas_respondidas")
df_alunos = df_alunos.drop('_c7')
df_alunos.cache().count()

spark.sql("DROP TABLE IF EXISTS dmd_alunos")
df_alunos.write.format("delta").saveAsTable('dmd_alunos')
