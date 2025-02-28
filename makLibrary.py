#%spark2.pyspark
#import datetime
#from pyspark.sql.types import StructType


with open('/home/tensor/zeppelin/my_functions.py', 'w') as f:
    f.write("""


from pyspark.sql import functions as F

from pyspark.sql.functions import *
from pyspark.sql.functions import second, lag, col, expr, sum, from_unixtime, from_utc_timestamp, udf, split, coalesce, lit, when, max, window, explode, split
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, TimestampType, StructField, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import concat
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
from pyspark.rdd import RDD
import os
import my_functions as My
from datetime import datetime, timedelta
import pandas as pd
from pyspark.storagelevel import StorageLevel


def get_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y/%m/%d")
    end_date = datetime.strptime(end_date_str, "%Y/%m/%d")
    
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    
    return [date.strftime("%Y/%m/%d") for date in date_range]



def SaveHadoop(start_date, end_date, savePath, df_Total):


#savePath
 #test1: 기본
 #test2: 시간차
 #test3: 안정화구간 & comp. efficiency 추가

    SYear = start_date.split("/")[0]
    SMonth = start_date.split("/")[1]
    SDay = start_date.split("/")[2]

    EYear = end_date.split("/")[0]
    EMonth = end_date.split("/")[1]
    

    tagName = SYear + SMonth + SDay + "_" +EYear + EMonth + "E"
    hadoop_path = "hdfs://10.224.81.60/data_tmp/YKIM/01_GT_RMS/{}/{}/{}/{}".format(savePath, SYear, SMonth, tagName)
    
    df_Total.write.option("compression", "gzip").mode("overwrite").parquet(hadoop_path)

    
    return print("저장완료")


def MakeDataframe1(spark, period, Tag_list):
    
    schema = StructType([])
    df_Total =  spark.createDataFrame([], schema = schema)

    for date in period:
       splited_date = date.split('/')
       year = int(splited_date[0])
       month = int(splited_date[1])
       day = int(splited_date[2])
    
    # 시작 시간과 종료 시간 설정
       start_time = "00:00:00"
       end_time = "23:59:59"

    # 시작 시간과 종료 시간을 초로 변환
       start_timestamp = spark.range(1).select(expr(f"unix_timestamp('{year:04d}-{month:02d}-{day:02d} {start_time}')").alias("start_timestamp")).first()["start_timestamp"]
       end_timestamp = spark.range(1).select(expr(f"unix_timestamp('{year:04d}-{month:02d}-{day:02d} {end_time}')").alias("end_timestamp")).first()["end_timestamp"]

    # 초 범위 생성
       seconds_range = spark.range(start_timestamp, end_timestamp + 1)
    
    # 초를 시간 형식으로 변환from_unixtime("id")는 "id" 컬럼에 저장된 Unix 타임스탬프 값을 
    # 날짜와 시간 형식으로 변환하고, alias("New_Date_Time")을 사용하여 새로운 컬럼의 이름을 "New_Date_Time"으로 지정
       df = seconds_range.select(from_unixtime("id").alias("Time"))
    
    # 날짜별 추출 Tag List 
       schema = StructType([])
       df_Total_day = spark.createDataFrame([], schema = schema)
       
       def convert_to_time(value):
       # 변환 로직 구현
       # 예시로 시분초 형식으로 변환하는 경우
          if value is None:
             return None
             
          hours = value // 3600
          minutes = (value % 3600) // 60
          seconds = value % 60
          return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
       
    for tag in Tag_list :
                
        hadoop_path = "hdfs://10.224.81.60/data0/plants/hanam/{:04d}/{:04d}_{:02d}/{:02d}_{:02d}_{:02d}/{}".format(year, year, month, year, month, day,tag)   
        #print(hadoop_path)
        #hadoop 경로 불러오기
        
        try:
            df_raw = spark.read.csv(hadoop_path, sep = "\t", header = False)
            
             # string 데이터 . 구분후 첫번째 값가져오기        
            tag_name = tag.split('.')[0]

            #df 컬럼명 생성
            df_raw = df_raw.drop('_c2') \
                           .withColumnRenamed('_c0', 'Time') \
                           .withColumnRenamed('_c1', tag_name)
            
            df_raw = df_raw.withColumn('Time', split(col('Time'), '\.')[0])\
                           .withColumn('Time',col('Time').cast('timestamp'))\
                           .withColumn(tag_name, col(tag_name).cast('float'))
            

            df_selected = df_raw.orderBy(col('Time').desc())\
                                .dropDuplicates(['Time'])

            # Join with the main dataframe
            df = df.join(df_selected, on='Time', how='left')
                      
        except AnalysisException:
            square_udf = udf(convert_to_time)
            print("{}, {}, {}, {} 파일 없음".format(year, month, day, tag ))
            df_tempor =spark.range(86401).toDF("seconds")
            df_tempor = df_tempor \
                        .withColumn("Time", square_udf(col("seconds")).cast("timestamp")) \
                        .drop("seconds") \
                        .withColumn(tag.split('.')[0], lit(None))
            df = df.join(df_tempor, "Time", 'left')
            
            pass
                   
    
    #날짜단위 데이터프레임 합치기
    if len(df_Total.columns) > 0 :
        df_Total = df_Total.unionAll(df)            
    else:
        df_Total = df
                   
    
    #날짜단위 데이터프레임 합치기
        if len(df_Total.columns) > 0 :
           df_Total = df_Total.unionAll(df)            
        else:
           df_Total = df
    
    
    df_Total= df_Total.dropna()
       
    return df_Total    


def MakeDataframe2(spark, period, Tag_list):

    schema = StructType([])
    df_Total =  spark.createDataFrame([], schema = schema)



    for date in period:
        splited_date = date.split('/')
        # 예 : year = splited_date[0]
    
        year = int(splited_date[0])
        month = int(splited_date[1])
        day = int(splited_date[2])
    
    # 시작 시간과 종료 시간 설정
        start_time = "00:00:00"
        end_time = "23:59:59"

    # 시작 시간과 종료 시간을 초로 변환
        start_timestamp = spark.range(1).select(expr(f"unix_timestamp('{year:04d}-{month:02d}-{day:02d} {start_time}')").alias("start_timestamp")).first()["start_timestamp"]
        end_timestamp = spark.range(1).select(expr(f"unix_timestamp('{year:04d}-{month:02d}-{day:02d} {end_time}')").alias("end_timestamp")).first()["end_timestamp"]

    # 초 범위 생성
        seconds_range = spark.range(start_timestamp, end_timestamp + 1)
    
    # 초를 시간 형식으로 변환from_unixtime("id")는 "id" 컬럼에 저장된 Unix 타임스탬프 값을 
    # 날짜와 시간 형식으로 변환하고, alias("New_Date_Time")을 사용하여 새로운 컬럼의 이름을 "New_Date_Time"으로 지정
        df = seconds_range.select(from_unixtime("id").alias("Time"))
    
    # 날짜별 추출 Tag List 
        schema = StructType([])
        df_Total_day = spark.createDataFrame([], schema = schema)
    
    
    
        for tag in Tag_list :
                
        # spark = SparkSession.builder.appName("HDFS Data Extraction").getOrCreate()
            hadoop_path = "hdfs://10.224.81.60/data0/plants/hanam/{:04d}/{:04d}_{:02d}/{:02d}_{:02d}_{:02d}/{}".format(year, year, month, year, month, day,tag)   
        #print(hadoop_path)
        #hadoop 경로 불러오기
        
            try:
               df_raw = spark.read.csv(hadoop_path, sep = "\t", header = False)
            
               # string 데이터 . 구분후 첫번째 값가져오기        
               tag_name = tag.split('.')[0]

            #df 컬럼명 생성
               df_raw = df_raw.drop('_c2') \
                           .withColumnRenamed('_c0', 'Time') \
                           .withColumnRenamed('_c1', tag_name)
            
               df_raw = df_raw.withColumn('Time', split(col('Time'), '\.')[0])\
                           .withColumn('Time',col('Time').cast('timestamp'))\
                           .withColumn(tag_name, col(tag_name).cast('float'))
            
            #Time Delay 반영
               if tag_name == "HN_GTC_11RCAOG_D002_01" :   # "Generator_Power_output"

                  df_raw = df_raw.withColumn('Time', expr('Time + INTERVAL 3 seconds'))

               elif tag_name == "HN_GTC_11RCAOG_B200_06":  # "Power Factor" :
                    df_raw = df_raw.withColumn('Time', expr('Time + INTERVAL 3 seconds'))

                    
#              elif tag_name == "HN_GTC_11RCAOG_C013_01" : # "11GT_No2_ROW_DISC_CAVITY_TEMPERATURE" :
#                   df_raw = df_raw.withColumn('Time', expr('Time + INTERVAL 2 seconds'))                
                
#              elif tag_name == 'HN_GTC_11RCAOG_C013_02' : # "11GT_No3_ROW_DISC_CAVITY_TEMPERATURE" :
#                   df_raw = df_raw.withColumn('Time', expr('Time + INTERVAL 8 seconds'))
           
#              elif tag_name == 'HN_GTC_11RCAOG_C013_03' : # "11GT_No4_ROW_DISC_CAVITY_TEMPERATURE" :
#                   df_raw = df_raw.withColumn('Time', expr('Time + INTERVAL 8 seconds'))

#              else :
#                   pass
            
               df_selected = df_raw.orderBy(col('Time').desc())\
                                   .dropDuplicates(['Time'])
            # Join with the main dataframe
               df = df.join(df_selected, on='Time', how='left')
            
              # print("저장완료")          
            except AnalysisException:
               square_udf = udf(convert_to_time)
               print("{}, {}, {}, {} 파일 없음".format(year, month, day, tag ))
               df_tempor =spark.range(86401).toDF("seconds")
               df_tempor = df_tempor \
                           .withColumn("Time", square_udf(col("seconds")).cast("timestamp")) \
                           .drop("seconds") \
                           .withColumn(tag.split('.')[0], lit(None))
               df = df.join(df_tempor, "Time", 'left')
               print("except")
               pass
                   
       #날짜단위 데이터프레임 합치기
        if len(df_Total.columns) > 0 :
           df_Total = df_Total.unionAll(df)            
        else:
           df_Total = df
    
        df_Total= df_Total.dropna()
        
    return df_Total


def MakeDataframe3(spark, period, Tag_list):
    Year, Month, Day = period[0].split("/")
    #print(period[0])
    #print(Year, Month)
    
    tagName = Year + Month + "01" + "_" +Year + Month + "E"

    hadoop_path_1 = "hdfs://10.224.81.60/data_tmp/YKIM/01_GT_RMS/test2/{}/{}/{}".format(Year, Month, tagName)
    df_Total = spark.read.parquet(hadoop_path_1).withColumn('Time', col('Time').cast(TimestampType())).orderBy("Time")
    df_Total= df_Total.dropna()

    #Tag 이름 변환
    path ="/home/tensor/data/YKim/HanamTagDesr.csv"
    df_input = pd.read_csv(path)

    df_input['TAG_DESC'] = df_input['TAG_DESC'].str.replace(' ','_')
    df_input['TAG_DESC'] = df_input['TAG_DESC'].str.replace('(','')
    df_input['TAG_DESC'] = df_input['TAG_DESC'].str.replace(')','')
    df_input['TAG_DESC'] = df_input['TAG_DESC'].str.replace('.','')

    df_dic = dict(zip(df_input['TAG_ID'],df_input['TAG_DESC']))
    for key, value in df_dic.items() :
        df_Total = df_Total.withColumnRenamed(key, value) 
     
    #데이터Type 변환
    for i in df_Total.columns:
    
        #데이터프레임의 데이터 type확인
        datatype = df_Total.schema[i].dataType.typeName()
    
        #print(datatype)
        if datatype == "string" :
           df_Total = df_Total.withColumn(i, col(i).cast("double"))

    df_Total.persist()
#    def adiabatic_efficiency(P1, P2, Air_Press, T1, T2):
#        P1 =  ((Air_Press)/760)*1.013
#        P2 = P2 + (Air_Press/760)*1.013
#        T1 = T1 + 273
#        T2 = T2 + 273
#        k = 1.4
#        eta = (((P2/P1)**((k-1)/k) - 1) / ((T2/T1) - 1)) * 100
#        return eta

    #Raw 데이터 전처리

    df_Normal = df_Total.filter(col("Generator_Power_output").between(100,280))
#    df_Normal.persist()
#    df_Total_fted = df_Total_fted.withColumn("Temp_deviation", col('11GT_COMPRESSOR_OUTLET_AIR_TEMPERATURE')-col('11GT_COMPRESSOR_INLET_AIR_TEMPERATURE'))
#    df_Total_fted = df_Total_fted.withColumn("Com_eff", adiabatic_efficiency( col('11GT_INLET_MANIFOLD_STATIC_PRESSURE'),col('11GT_COMBUSTOR_SHELL_PRESSURE'), col('11GT_BAROMETRIC_PRESSURE'), col('11GT_COMPRESSOR_INLET_AIR_TEMPERATURE'), col('11GT_COMPRESSOR_OUTLET_AIR_TEMPERATURE')))
#    df_Normal = df_Total_fted.filter(col("Com_eff").between(0, 99))
#    df_Normal = df_Normal.withColumn('VA', col("Generator_Power_output")/(col('11GT_GEN_POWER_FACTOR') * (col('Com_eff')/100)))

#    df_Normal.orderBy('Time')
     
    

    windowSpec = Window.orderBy("Time").rowsBetween(-50,0) #이전 100
    df_spark = df_Normal.withColumn("moving_average", F.avg("Generator_Power_output").over(windowSpec))

    tolerance = 0.001
    df_spark = df_spark.withColumn("change_percentage", (col("Generator_Power_output") - col("moving_average")) / col("moving_average"))
    stable_intervals = df_spark.filter(abs(col("change_percentage")) < tolerance).select("Time", "Generator_Power_output")

    DF_StableIntervals= stable_intervals.toPandas()
    DF_Total = df_Total.toPandas()

    start_time = '{}-{}-19 00:01:00'.format(Year, Month)
    end_time = '{}-{}-23 23:59:00'.format(Year, Month)
    DF_StableSection = DF_StableIntervals[(DF_StableIntervals['Time'] >= start_time) & (DF_StableIntervals['Time'] <= end_time)]

    DF_TotalSection = DF_Total[(DF_Total['Time'] >= start_time) & (DF_Total['Time'] <= end_time)]



    windowSpec = Window.orderBy("Time").rowsBetween(-5,0)
    stableInterval_stage2 = stable_intervals.withColumn("moving_average", F.avg("Generator_Power_output").over(windowSpec))

    tolerance = 0.005 #ㅏ 0.009 좋은
    stableInterval_stage2 = stableInterval_stage2.withColumn("change_percentage", (col("Generator_Power_output") - col("moving_average")) / col("moving_average"))
    stableInterval_stage2 = stableInterval_stage2.filter(abs(col("change_percentage")) < tolerance).select("Time", "Generator_Power_output")

#    stableInterval_stage2.persist()  

    # 이전 행의 값을 구하기 위해 Window 함수 작성
    window_spec = Window.orderBy("Time")

    stableInterval_stage3 = stableInterval_stage2.withColumn("Prev_Output", lag("Generator_Power_output").over(window_spec))
    stableInterval_stage3 = stableInterval_stage3.withColumn("Change", F.abs(col("Generator_Power_output") - col("Prev_Output")) / col("Prev_Output"))

    # 시간간격 5분 이상 차이 발생시 Is_stable 추가
    stableInterval_stage3 = stableInterval_stage3.withColumn("Prev_Time", F.lag("Time").over(window_spec))
    stableInterval_stage3 = stableInterval_stage3.withColumn("Time_diff", (F.col("Time").cast('long') - F.col("Prev_Time").cast('long')))

    # 변화량이 2% 미만인지 확인
    stableInterval_stage3 = stableInterval_stage3.withColumn("Is_Stable", col("Change") < 0.01) #이전 0.009
    stableInterval_stage3 = stableInterval_stage3.withColumn("No_data", F.col("Time_diff") < 100)

    # 각 구간의 번호 매기기
    stableInterval_stage3 = stableInterval_stage3.withColumn("GroupID_Indicator", when(col("Is_Stable") == False, 1).otherwise(0))
    stableInterval_stage3 = stableInterval_stage3.withColumn("GroupID", F.sum("GroupID_Indicator").over(Window.orderBy("Time").rowsBetween(Window.unboundedPreceding, 0)))

    stableInterval_stage3 = stableInterval_stage3.withColumn("SplitPoint_Indicator", when(col("No_data") == False, 1).otherwise(0))
    stableInterval_stage3 = stableInterval_stage3.withColumn("SplitPoint", F.sum("SplitPoint_Indicator").over(Window.orderBy("Time").rowsBetween(Window.unboundedPreceding, 0)))

    # 데이터 프레임 스키마 정의 및 빈 데이터프레임 생성
    schema = StructType([
             StructField('StartTime', TimestampType(), True),
             StructField('EndTime', TimestampType(), True),
             StructField('AvgOutput', FloatType(), True),
             StructField('MaxOutput', FloatType(), True),
             StructField('MinOutput', FloatType(), True)
             ])

    Summary_2stage = spark.createDataFrame([], schema=schema)

    # 필요한 구간 집계 처리
    group_aggregations = stableInterval_stage3.groupBy("SplitPoint", "GroupID").agg(
                     expr("min(Time)").alias("StartTime"),
                     expr("max(Time)").alias("EndTime"),
                     expr("avg(Generator_Power_output)").alias("AvgOutput"),
                     expr("max(Generator_Power_output)").alias("MaxOutput"),
                     expr("min(Generator_Power_output)").alias("MinOutput")
)

    # 검증된 내용을 최종 데이터프레임에 추가
    Summary_2stage = Summary_2stage.union(group_aggregations.select("StartTime", "EndTime", "AvgOutput", "MaxOutput", "MinOutput"))



    df_Normal.createOrReplaceGlobalTempView("sql_Reference")
    table = "global_temp.sql_Reference"
    sql_table =spark.table(table)


    # Pandas DataFrame으로 변환하여 'StartTime'과 'EndTime' 추출
    summary_pd_df = Summary_2stage.select('StartTime', 'EndTime').toPandas()

    # 빈 DataFrame 생성
    df_reference = spark.createDataFrame([], schema=df_Normal.schema)

    # Chunk size to process conditions in smaller batches
    chunk_size = 1000  
    # 적절한 chunk_size 설정
    # Create a list to store conditions
    filter_conditions = []

    # Create a filter condition for groups of time intervals
    for idx, row in summary_pd_df.iterrows():
        start = row['StartTime'].strftime("%Y-%m-%d %H:%M:%S")
        end = row['EndTime'].strftime("%Y-%m-%d %H:%M:%S")
        condition = f"(Time > '{start}' AND Time < '{end}')"
        filter_conditions.append(condition)

    # If the chunk is full, process it
        if len(filter_conditions) >= chunk_size:
           query = "SELECT * FROM global_temp.sql_Reference WHERE " + " OR ".join(filter_conditions)
        
        
        # SQL 쿼리 실행
           filtered_chunk = spark.sql(query)
           df_reference = df_reference.union(filtered_chunk)
        
        # Reset the filter_conditions list
           filter_conditions = []

    # Process remaining conditions
    if len(filter_conditions) > 0:
       query = "SELECT * FROM global_temp.sql_Reference WHERE " + " OR ".join(filter_conditions)
    
      # SQL 쿼리 실행
       filtered_chunk = spark.sql(query)
       df_reference = df_reference.union(filtered_chunk)
       
    return df_reference



#효율반영 
def AddEfficiency(spark, start_date):
    
    def Efficiency(P1, P2, Air_Press, T1, T2):
        P1 =  ((Air_Press)/760)*1.013
        P2 = P2 + (Air_Press/760)*1.013
        T1 = T1 + 273
        T2 = T2 + 273
        k = 1.4
        
        eta = (((P2/P1)**((k-1)/k)-1)/((T2/T1)-1))*100
        
        return eta

    Year, Month, Day = start_date.split("/")
    tagName = Year + Month + "01" + "_" +Year + Month + "E"
    hadoop_path= "hdfs://10.224.81.60/data_tmp/YKIM/01_GT_RMS/test3/{}/{}/{}".format(Year, Month, tagName)
    print(hadoop_path)
    df = spark.read.parquet(hadoop_path).withColumn('Time', col('Time').cast(TimestampType())).orderBy("Time")
    df = df.filter(col("Generator_Power_output").between(100,280))
    print(df.count())
    
    df_Result = df.withColumn("Com_eff", Efficiency(col('11GT_INLET_MANIFOLD_STATIC_PRESSURE'),col('11GT_COMBUSTOR_SHELL_PRESSURE'), col('11GT_BAROMETRIC_PRESSURE'), col('11GT_COMPRESSOR_INLET_AIR_TEMPERATURE'), col('11GT_COMPRESSOR_OUTLET_AIR_TEMPERATURE')))
    
    return df_Result


""")
