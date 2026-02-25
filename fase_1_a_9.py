from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
import uuid
from datetime import datetime
import boto3
import sys


def job_qualtrics_test():
    # ─────────────────────────────────────────────
    # CREAR CONTEXTOS SPARK Y GLUE
    # ─────────────────────────────────────────────
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # ─────────────────────────────────────────────
    # CONFIGURACIONES DE OPTIMIZACIÓN DE SPARK (obligatorias)
    # ─────────────────────────────────────────────
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
        spark.conf.set("spark.sql.shuffle.partitions", "120")
        spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")
        spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")
        spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "32")
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.adaptive.maxNumPostShufflePartitions", "120")
        print("Configuraciones de Spark optimizadas aplicadas")
    except (AttributeError, TypeError, ValueError, RuntimeError) as e:
        print(f"Advertencia: No se pudieron aplicar algunas configuraciones: {str(e)}")

    # ─────────────────────────────────────────────
    # CONFIGURACIÓN
    # ─────────────────────────────────────────────
    args = getResolvedOptions(sys.argv, ["environment"])
    var_env = args["environment"]
    connection_name = f"cdata-bocc-{var_env}-glue-db-catalog-redshift"
    temp_dir = f"s3://cdata-bocc-{var_env}-output/aplicaciones/apl_cdata/salida/qualtrics/"

    # -------------------------
    # Helpers de formato (una sola vez)
    # -------------------------
    def proper_case(col):
        return F.concat(F.upper(F.substring(col, 1, 1)), F.lower(F.substring(col, 2, 1000)))

    def collapse_spaces(col):
        return F.regexp_replace(F.trim(col), r"\s+", " ")

    def build_fullname(first_col, last_col, maiden_col):
        full = collapse_spaces(F.concat_ws(
            " ",
            F.coalesce(first_col, F.lit("")),
            F.coalesce(last_col, F.lit("")),
            F.coalesce(maiden_col, F.lit(""))
        ))
        return proper_case(full)

    def clean_city_desc(desc_col):
        cleaned = collapse_spaces(F.regexp_replace(desc_col, r"\.+\s*$", ""))
        return proper_case(cleaned)

    # ─────────────────────────────────────────────
    # HELPER: leer desde Redshift
    # ─────────────────────────────────────────────
    def leer_redshift(query: str):
        return glueContext.create_dynamic_frame_from_options(
            connection_type="redshift",
            connection_options={
                "connectionName": connection_name,
                "query": query,
                "redshiftTmpDir": temp_dir,
                "useConnectionProperties": True,
            },
        ).toDF()

    # ─────────────────────────────────────────────
    # FASE 1 — PN_BASE_LIGHT
    # ─────────────────────────────────────────────
    def get_query_pn_base_light():
        return """
        SELECT
            c.ROW_ID AS ROW_ID_CLIENTE,
            c.X_OCS_ID_NUM AS NUMERO_DE_DOCUMENTO,
            c.X_OCS_ID_TYPE AS TIPO_DOC_NAME,
            c.FST_NAME AS C_FST_NAME,
            c.LAST_NAME AS C_LAST_NAME,
            c.MAIDEN_NAME AS C_MAIDEN_NAME,
            c.PR_PER_ADDR_ID,
            c.PR_PHONE_ID,
            c.PR_POSTN_ID,
            pcon.POSTN_ID,
            postn.OU_ID AS ORG_ID,
            org.LOC AS COD_DIVISION_GERENTE,
            org.NAME AS ORG_NAME,
            postn.NAME AS POSTN_NAME,
            emp.ROW_ID AS GERENTE_ROW_ID,
            emp.EMP_NUM AS CODIGO_SAP_GERENTE,
            emp.FST_NAME AS G_FST_NAME,
            emp.LAST_NAME AS G_LAST_NAME,
            emp.MAIDEN_NAME AS G_MAIDEN_NAME,
            conx.ATTRIB_39 AS TIPO_ATENCION_NAME,
            per.ADDR AS DIRECCION,
            per.CITY AS CITY_VAL,
            fnxm.ATTRIB_49 AS SEGMENTO_NAME,
            fnxm.ATTRIB_50 AS SEGMENTO_FLAG,
            catalog.DETAIL_TYPE_CD AS CLASE_PRODUCTO_VAL
        FROM ADMSIEBEL.S_POSTN_CON pcon
        JOIN ADMSIEBEL.S_POSTN postn ON pcon.POSTN_ID = postn.ROW_ID
        JOIN ADMSIEBEL.S_CONTACT c ON pcon.CON_ID = c.ROW_ID
        JOIN ADMSIEBEL.S_ORG_EXT org ON postn.OU_ID = org.ROW_ID
        JOIN ADMSIEBEL.S_CONTACT emp ON postn.PR_EMP_ID = emp.ROW_ID
        JOIN ADMSIEBEL.S_EMPLOYEE_X t7 ON t7.PAR_ROW_ID = emp.ROW_ID
        LEFT JOIN ADMSIEBEL.S_CONTACT_X conx ON conx.PAR_ROW_ID = c.ROW_ID
        LEFT JOIN ADMSIEBEL.S_ADDR_PER per ON per.ROW_ID = c.PR_PER_ADDR_ID
        JOIN ADMSIEBEL.S_ASSET_CON rel ON rel.CONTACT_ID = c.ROW_ID
        JOIN ADMSIEBEL.S_ASSET prod ON rel.ASSET_ID = prod.ROW_ID
        JOIN ADMSIEBEL.S_PROD_INT catalog ON prod.PROD_ID = catalog.ROW_ID
        JOIN ADMSIEBEL.S_CONTACT_FNXM fnxm ON c.ROW_ID = fnxm.PAR_ROW_ID
        WHERE c.PR_POSTN_ID = pcon.POSTN_ID
          AND fnxm.ATTRIB_50 = '1000001'
          AND org.NAME <> 'DIRECCION ONBOARDING Y ASIGNACION'
          AND postn.NAME <> 'Siebel Administrator'
          AND prod.STATUS_CD IN ('1010000')
          AND rel.RELATION_TYPE_CD IN ('Deudor','Titular principal producto')
        """.strip()

    query_PN_BASE_LIGHT = get_query_pn_base_light()
    PN_base = leer_redshift(query_PN_BASE_LIGHT)
    print("Hasta Fase 1 OK")

    # ─────────────────────────────────────────────
    # FASE 2 — LOVs
    # ─────────────────────────────────────────────
    q_lov_multi = """
        SELECT TYPE AS TYPE_NAME, NAME, VAL, HIGH, DESC_TEXT
        FROM ADMSIEBEL.S_LST_OF_VAL
        WHERE TYPE IN (
            'OCS_NID_TYPES',
            'OCS_SEGMENT',
            'OCS_CITY',
            'FINCORP_PROD_ADMIN_CLASS_MLOV',
            'OCS_ATTENTION_TYPE'
        )
        AND ACTIVE_FLG = 'Y'
    """.strip()

    lov_all = (
        leer_redshift(q_lov_multi)
        .withColumnRenamed("NAME", "LOV_NAME")
        .withColumnRenamed("VAL", "LOV_VAL")
        .withColumnRenamed("HIGH", "LOV_HIGH")
        .withColumnRenamed("DESC_TEXT", "LOV_DESC")
    )

    # Particionarlo en 5 DataFrames según el TYPE
    lov_nid     = lov_all.filter(F.col("TYPE_NAME") == "OCS_NID_TYPES").select("LOV_NAME", "LOV_HIGH")
    lov_segment = lov_all.filter(F.col("TYPE_NAME") == "OCS_SEGMENT").select("LOV_NAME", "LOV_DESC")
    lov_city    = lov_all.filter(F.col("TYPE_NAME") == "OCS_CITY").select("LOV_VAL", "LOV_DESC")
    lov_prod    = lov_all.filter(F.col("TYPE_NAME") == "FINCORP_PROD_ADMIN_CLASS_MLOV").select("LOV_VAL", "LOV_DESC")
    lov_att     = lov_all.filter(F.col("TYPE_NAME") == "OCS_ATTENTION_TYPE").select("LOV_NAME", "LOV_DESC")

    print("lov_all:", lov_all.count())
    print("Hasta Fase 2 OK")

    # ─────────────────────────────────────────────
    # FASE 3 — ENRIQUECIMIENTO
    # ─────────────────────────────────────────────
    PN_enriched = (
        PN_base
        .join(broadcast(lov_nid).alias("lv_nid"),
              F.col("TIPO_DOC_NAME") == F.col("lv_nid.LOV_NAME"), "left")
        .withColumn("TIPO_DE_DOCUMENTO_COD", F.col("lv_nid.LOV_HIGH"))

        .join(broadcast(lov_segment).alias("lv_seg"),
              F.col("SEGMENTO_NAME") == F.col("lv_seg.LOV_NAME"), "left")
        .withColumn("SEGMENTO_COMERCIAL_CLIENTE", F.col("lv_seg.LOV_DESC"))

        .join(broadcast(lov_att).alias("lv_att"),
              F.col("TIPO_ATENCION_NAME") == F.col("lv_att.LOV_NAME"), "left")
        .withColumn("TIPO_ATENCION_DESC", F.col("lv_att.LOV_DESC"))

        .join(broadcast(lov_city).alias("lv_city"),
              F.col("CITY_VAL") == F.col("lv_city.LOV_VAL"), "left")
        .withColumn("CIUDAD_DESC", clean_city_desc(F.col("lv_city.LOV_DESC")))

        .join(broadcast(lov_prod).alias("lv_prod"),
              F.col("CLASE_PRODUCTO_VAL") == F.col("lv_prod.LOV_VAL"), "left")
        .withColumn("CLASE_PRODUCTO_DESC", F.col("lv_prod.LOV_DESC"))

        .withColumn("NOMBRE_CLIENTE",
                    build_fullname(F.col("C_FST_NAME"), F.col("C_LAST_NAME"), F.col("C_MAIDEN_NAME")))
        .withColumn("NOMBRE_GERENTE",
                    build_fullname(F.col("G_FST_NAME"), F.col("G_LAST_NAME"), F.col("G_MAIDEN_NAME")))

        .filter(F.col("SEGMENTO_COMERCIAL_CLIENTE").isin(
            'BP - Selecto','BP - Preferente Plus','BP - Elite Plus',
            'BP - Masivo','BP - Mi Grupo es Aval','BP - Preferente','BP - Elite'
        ))
        .select(
            "ROW_ID_CLIENTE","NUMERO_DE_DOCUMENTO",
            "TIPO_DE_DOCUMENTO_COD",
            "NOMBRE_CLIENTE",
            "COD_DIVISION_GERENTE",
            "NOMBRE_GERENTE",
            "CODIGO_SAP_GERENTE",
            F.col("SEGMENTO_COMERCIAL_CLIENTE").alias("SEGMENTO"),
            F.col("TIPO_ATENCION_DESC").alias("TIPO_ATENCION"),
            "DIRECCION",
            F.col("CIUDAD_DESC").alias("CIUDAD"),
            F.col("CLASE_PRODUCTO_DESC").alias("CLASE_PRODUCTO"),
            "PR_PHONE_ID"
        )
    )
    print("Hasta Fase 3 OK")

    # ─────────────────────────────────────────────
    # FASE 4 — PHONES LIGHT
    # ─────────────────────────────────────────────
    query_PHONES_LIGHT = """
        SELECT
            p.CON_ID          AS ORG_ID,
            p.ROW_ID          AS PHONE_ROW_ID,
            p.X_OCS_PHONE_NUM,
            p.X_OCS_EXTENSION,
            p.X_OCS_END_DATE
        FROM ADMSIEBEL.S_CON_PHONE p
        WHERE p.X_OCS_END_DATE IS NULL
    """.strip()

    phones_raw = leer_redshift(query_PHONES_LIGHT)
    print("phones_raw:", phones_raw.count())
    print("Hasta Fase 4 OK")

    # ─────────────────────────────────────────────
    # FASE 5 TELÉFONOS: agregación por cliente + LEFT JOIN
    # ─────────────────────────────────────────────

    # Opcional repartition
    # PN_enriched = PN_enriched.repartition(120, "ROW_ID_CLIENTE")
    # phones_raw  = phones_raw.repartition(120, "ORG_ID")

    phones_pre = (
        phones_raw.alias("p")
        .withColumn("PHONE_NUM_TRIM", F.trim(F.col("p.X_OCS_PHONE_NUM")))
        .filter(F.col("PHONE_NUM_TRIM").isNotNull() & (F.length(F.col("PHONE_NUM_TRIM")) > 0))
        .withColumn("PHONE_FORMAT", F.col("PHONE_NUM_TRIM"))
        .withColumn("PHONE_ORDER", F.col("PHONE_NUM_TRIM"))
    )

    pn_keys = PN_enriched.select("ROW_ID_CLIENTE", "PR_PHONE_ID").distinct()
    pn_keys = broadcast(pn_keys)

    phones_labeled = (
        phones_pre.alias("p")
        .join(
            pn_keys.alias("c"),
            F.col("p.ORG_ID") == F.col("c.ROW_ID_CLIENTE"),
            "left"
        )
        .withColumn(
            "PHONE_FORMAT",
            F.when(
                F.col("p.PHONE_ROW_ID") == F.col("c.PR_PHONE_ID"),
                F.concat(F.lit("P-"), F.col("PHONE_FORMAT"))
            ).otherwise(F.col("PHONE_FORMAT"))
        )
    )

    phones_agg = (
        phones_labeled
        .groupBy("ORG_ID")
        .agg(
            F.expr("""
                array_join(
                    transform(
                        array_sort(collect_list(named_struct('k', PHONE_ORDER, 'v', PHONE_FORMAT))),
                        x -> x.v
                    ),
                    ', '
                )
            """).alias("TELEFONOS")
        )
    )

    PN_with_phones = (
        PN_enriched.alias("pn")
        .join(
            phones_agg.alias("ph"),
            F.col("pn.ROW_ID_CLIENTE") == F.col("ph.ORG_ID"),
            "left"
        )
        .drop("ORG_ID")
    )

    print(f"[FASE 5] PN_with_phones: {PN_with_phones.count():,} registros")
    print("Hasta Fase 5 OK")

    # ─────────────────────────────────────────────
    # FASE 6: Classes agg + Final DF
    # ─────────────────────────────────────────────

    classes_uniq = PN_enriched.dropDuplicates([
        "ROW_ID_CLIENTE","TIPO_DE_DOCUMENTO_COD","NUMERO_DE_DOCUMENTO","NOMBRE_CLIENTE",
        "COD_DIVISION_GERENTE","NOMBRE_GERENTE","CODIGO_SAP_GERENTE",
        "SEGMENTO","TIPO_ATENCION","DIRECCION","CIUDAD","CLASE_PRODUCTO"  # ✅ Mantiene multiples clases
    ])

    classes_agg_client = (
        classes_uniq.groupBy(
            "ROW_ID_CLIENTE","TIPO_DE_DOCUMENTO_COD","NUMERO_DE_DOCUMENTO","NOMBRE_CLIENTE",
            "COD_DIVISION_GERENTE","NOMBRE_GERENTE","CODIGO_SAP_GERENTE",
            "SEGMENTO","TIPO_ATENCION","DIRECCION","CIUDAD"
        ).agg(
            F.array_join(
                F.array_sort(F.array_distinct(F.collect_list("CLASE_PRODUCTO"))),
                ", "
            ).alias("CLASES_PRODUCTO")
        )
    )

    PN_df = (
        classes_agg_client.alias("c")
        .join(phones_agg.alias("p"), F.col("c.ROW_ID_CLIENTE") == F.col("p.ORG_ID"), "left")
        .select(
            F.col("c.SEGMENTO").alias("SEGMENTO"),
            F.col("c.TIPO_DE_DOCUMENTO_COD").alias("TIPO DE DOCUMENTO"),
            F.col("c.NUMERO_DE_DOCUMENTO").alias("IDENT"),
            F.col("c.NOMBRE_CLIENTE").alias("CLIENTE"),
            F.col("c.TIPO_ATENCION").alias("CAPA"),
            F.col("c.COD_DIVISION_GERENTE").alias("CODIGO ZONAL"),
            F.col("c.CODIGO_SAP_GERENTE").alias("CODIGO SAP GERENTE"),
            F.col("c.NOMBRE_GERENTE").alias("NOMBRE_GERENTE"),
            F.col("c.CLASES_PRODUCTO").alias("PRODUCTO"),
            F.coalesce(F.col("p.TELEFONOS"), F.lit("N/A")).alias("TELEFONOS"),  # ✅ Null-safe
            F.col("c.DIRECCION").alias("DIRECCION"),
            F.col("c.CIUDAD").alias("CIUDAD")
        ).orderBy("CLIENTE")
    )

    print(f"Final rows: {PN_df.count():,}")
    print("Hasta Fase 6 OK")

    # ─────────────────────────────────────────────
    # FASE 7: LEY 1581 - Lectura + Latest por Doc
    # ─────────────────────────────────────────────

    query_MDM1581 = """
        SELECT V.CONTENT AS CONTACTO, G.REF_NUM AS NUM_ID_CLIENTE, B.AGREE_IND, B.CREATE_DATE AS FECHA_DILIG
        FROM ADMMDM.CONTACT CT INNER JOIN ADMMDM.IDENTIFIER G ON CT.CONT_ID = G.CONT_ID
        INNER JOIN ADMMDM.CONSENT B ON B.CONSENT_OWNER_ID = G.CONT_ID
        INNER JOIN ADMMDM.PROCESSINGPURPOSE P ON B.PROC_PURP_ID = P.PROC_PURP_ID
        INNER JOIN ADMMDM.CONSENTREGULATION CR ON B.PROC_PURP_ID = CR.PROC_PURP_ID 
        INNER JOIN ADMMDM.PROCESSINGACTIVITY PA ON B.PROC_PURP_ID = PA.PROC_PURP_ID
        INNER JOIN ADMMDM.PROCPURPTENANTASSOC PPTA ON B.PROC_PURP_ID = PPTA.PROC_PURP_ID
        LEFT JOIN ADMMDM.CONSENTPROVISION V ON B.CONSENT_ID = V.CONSENT_ID
        WHERE P.PROC_PURP_TP_CD = '1000002' AND REGULATION_TP_CD = '1000001'
        AND PROC_ACT_TP_CD = '1000001' AND TENANT_TP_CD = '1000007'
        AND B.END_DT IS NULL AND CT.INACTIVATED_DT IS NULL AND G.END_DT IS NULL
    """.strip()

    MDM1581 = leer_redshift(glueContext, connection_name, temp_dir, query_MDM1581)

    MDM1581_latest = (
        MDM1581.withColumn("doc_join", F.regexp_replace(F.col("NUM_ID_CLIENTE").cast("string"), r"\\s+", ""))
        .withColumn("doc_join", F.regexp_replace(F.col("doc_join"), r"^0+", ""))
        .withColumn("ts", F.coalesce(
            F.to_timestamp(F.col("FECHA_DILIG").cast("string"), "dd/MM/yy hh:mm:ss,SSSSSSSSS a"),
            F.to_timestamp(F.col("FECHA_DILIG").cast("string"), "dd/MM/yy hh:mm:ss a"),
            F.to_timestamp(F.col("FECHA_DILIG").cast("string"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("FECHA_DILIG"))
        ))
        .withColumn("agree", F.when(F.col("AGREE_IND").cast("int") == 0, 0).otherwise(1))
        .withColumn("rn", F.row_number().over(Window.partitionBy("doc_join").orderBy(F.col("ts").desc_nulls_last())))
        .filter(F.col("rn") == 1)
        .select(F.lit("1581").alias("ley"), "doc_join", "agree", "ts", F.col("CONTACTO").alias("canal_contacto"))
        .drop("rn")
    )

    print(f"[FASE 7] 1581_latest: {MDM1581_latest.count():,} consents")
    print("Fase 7 OK")

    # ─────────────────────────────────────────────
    # FASE 8: LEY 2300 - Lectura + Latest por Doc
    # ─────────────────────────────────────────────

    query_LEY2300 = """
        SELECT I.REF_NUM AS NUM_ID_CLIENTE, C.CREATE_DATE AS FECHA_DE_INICIO, C.AGREE_IND,
            NVL(V.CONTENT,'-') AS CANAL_DE_CONTACTO
        FROM ADMMDM.CONTACT CT INNER JOIN ADMMDM.IDENTIFIER I ON CT.CONT_ID = I.CONT_ID
        INNER JOIN ADMMDM.CONSENT C ON I.CONT_ID = C.CONSENT_OWNER_ID
        INNER JOIN ADMMDM.CONSENTPROVISION V ON C.CONSENT_ID = V.CONSENT_ID
        INNER JOIN ADMMDM.PROCESSINGPURPOSE P ON C.PROC_PURP_ID = P.PROC_PURP_ID
        INNER JOIN ADMMDM.CONSENTREGULATION CR ON C.PROC_PURP_ID = CR.PROC_PURP_ID 
        INNER JOIN ADMMDM.PROCESSINGACTIVITY PA ON C.PROC_PURP_ID = PA.PROC_PURP_ID
        INNER JOIN ADMMDM.PROCPURPTENANTASSOC PPTA ON C.PROC_PURP_ID = PPTA.PROC_PURP_ID
        WHERE P.PROC_PURP_TP_CD = '1000002' AND REGULATION_TP_CD = '1000003'
        AND PROC_ACT_TP_CD = '1000001' AND TENANT_TP_CD = '1000007'
        AND C.END_DT IS NULL AND CT.INACTIVATED_DT IS NULL AND I.END_DT IS NULL
    """.strip()  # Simplificado cols clave

    LEY2300_latest = (
        leer_redshift(glueContext, connection_name, temp_dir, query_LEY2300)
        .withColumn("doc_join", F.regexp_replace(F.col("NUM_ID_CLIENTE").cast("string"), r"\\s+", ""))
        .withColumn("doc_join", F.regexp_replace(F.col("doc_join"), r"^0+", ""))
        .withColumn("ts", F.coalesce(
            F.to_timestamp(F.col("FECHA_DE_INICIO").cast("string"), "dd/MM/yy hh:mm:ss,SSSSSSSSS a"),
            F.to_timestamp(F.col("FECHA_DE_INICIO").cast("string"), "dd/MM/yy hh:mm:ss a"),
            F.to_timestamp(F.col("FECHA_DE_INICIO").cast("string"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("FECHA_DE_INICIO"))
        ))
        .withColumn("agree", F.when(F.trim(F.col("AGREE_IND")) == "0", 0)
                        .when(F.trim(F.col("AGREE_IND")) == "1", 1)
                        .otherwise(F.col("AGREE_IND").cast("int")))
        .withColumn("rn", F.row_number().over(Window.partitionBy("doc_join").orderBy(F.col("ts").desc_nulls_last())))
        .filter(F.col("rn") == 1)
        .select(F.lit("2300").alias("ley"), "doc_join", "agree", "ts", F.col("CANAL_DE_CONTACTO").alias("canal_contacto"))
        .drop("rn")
    )

    print(f"[FASE 8] 2300_latest: {LEY2300_latest.count():,} consents")
    print("Fase 8 OK")

    # ─────────────────────────────────────────────
    # FASE 9: Union Leyes + Latest + Filter + Join + Write
    # ─────────────────────────────────────────────

    consent_candidates = MDM1581_latest.unionByName(LEY2300_latest)

    consent_latest = (
        consent_candidates.withColumn("rn_all", F.row_number().over(
            Window.partitionBy("doc_join").orderBy(
                F.col("ts").desc_nulls_last(),
                F.when(F.col("ley") == "2300", 1).otherwise(0).desc()
            )
        )).filter(F.col("rn_all") == 1).select("doc_join", "ley", "agree", "ts", "canal_contacto")
    )

    consent_excluir = consent_latest.filter(F.col("agree") == 0).select("doc_join").distinct()
    consent_ok = consent_latest.filter(F.col("agree") != 0).select("doc_join", "canal_contacto")

    # Doc_join en PN_df (asumiendo ROW_ID_CLIENTE como ID cliente)
    PN_with_doc = PN_df.withColumn("doc_join",
        F.regexp_replace(F.col("ROW_ID_CLIENTE").cast("string"), r"\\s+", "")
    ).withColumn("doc_join", F.regexp_replace(F.col("doc_join"), r"^0+", ""))

    # Exclude no-consent + add canal
    PN_filtered = PN_with_doc.join(consent_excluir, on="doc_join", how="left_anti")
    PN_final = PN_filtered.alias("pn").join(
        consent_ok.alias("cs"), on="doc_join", how="left"
    ).select(
        "SEGMENTO", F.col("`TIPO DE DOCUMENTO`"), "IDENT", "CLIENTE", "CAPA",
        F.col("`CODIGO ZONAL`"), F.col("`CODIGO SAP GERENTE`"), "NOMBRE_GERENTE",
        "PRODUCTO", "TELEFONOS", "DIRECCION", "CIUDAD",
        F.coalesce(F.col("cs.canal_contacto"), F.lit("N/A")).alias("CANAL_DE_CONTACTO")
    ).drop("doc_join")

    print(f"[FASE 9] consent_latest: {consent_latest.count():,}, excluir: {consent_excluir.count():,}")
    print(f"[FASE 9] PN_final: {PN_final.count():,} registros")

    # Write CSV S3 (estilo simple)
    from datetime import datetime
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"{temp_dir}PN_LEYES_1581_2300_{now_str}/"
    PN_final.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"✅ Escrito en: {output_path}")

    PN_final.limit(10).show(truncate=False)
    print("Job completo OK")

    job_qualtrics_test()
