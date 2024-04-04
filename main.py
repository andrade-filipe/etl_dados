from pyspark.sql import SparkSession

# • Produtos mais vendidos
# • Faturamento total
# • Faturamento por categoria e por produto
# • Maiores comissões de vendedores
# • Quantidade de Fornecedores por estado
# • Quantidade de clientes por estado
# • Todas as representações devem estar por ano, trimestre e mês.
# • Todas as datas devem estar no formato YYYYMMDD
# • Todos os textos precisam estar em maiúsculo
# • Embora não esteja no sistema OLTP, no DW será preciso criar um campo "region" para guardar a região
# do estado.
# • É preciso calcular e armazenar o subtotal por item de venda.

def getPublicCategories(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.categories") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()

def getPublicCustomers(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.customers") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()

def getPublicProducts(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.products") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()
        
def getPublicSales(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.sales") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()
        
def getPublicSalesItems(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.sales_items") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()
        
def getPublicSellers(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.sellers") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()
        
def getPublicSuppliers(spark):
    return spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://dpg-co5jfb4f7o1s73a319ag-a.oregon-postgres.render.com:5432/fatorv") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.suppliers") \
        .option("user", "root") \
        .option("password", "vW36eDzFKnl2h2ZFCWo7eqgVth9gMC4x") \
        .load()

def main():
    spark = SparkSession.builder.appName("lab_dados") \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
        .getOrCreate()
    
    public_categories = getPublicCategories(spark)
    public_customers = getPublicCustomers(spark)
    public_products = getPublicProducts(spark)
    public_sales = getPublicSales(spark)
    public_sales_items = getPublicSalesItems(spark)
    public_sellers = getPublicSellers(spark)
    public_suppliers = getPublicSuppliers(spark)
        
    
    public_categories.printSchema()
    public_customers.printSchema()
    public_products.printSchema()
    public_sales.printSchema()
    public_sales_items.printSchema()
    public_sellers.printSchema()
    public_suppliers.printSchema()
    
if __name__ == "__main__":
    main()