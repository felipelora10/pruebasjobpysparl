lamada recursiva: job_qualtrics_test() está al final del archivo pero dentro del def, lo que dispara recursión infinita. (Mover afuera). [bancoccide...epoint.com]
Uso de la función leer_redshift con firma equivocada en Fase 7 y 8 (le pasas 4 argumentos, pero está definida para 1). (Usar solo leer_redshift(query)). [bancoccide...epoint.com]
Construcción de PN_with_doc: en Fase 9 calculas doc_join desde ROW_ID_CLIENTE, pero esa columna ya no existe en PN_df (la perdiste al seleccionar columnas “bonitas”). (Solución: o conservar ROW_ID_CLIENTE en PN_df, o calcular doc_join desde IDENT). [bancoccide...epoint.com]
Operador <> en SQL escapado como \<\>** dentro del string** (no rompe Python, pero en Redshift debe ser <>). (Recomendación: reemplazar por <> para que la query sea exactamente la que esperas). [bancoccide...epoint.com]
Uso de \> dentro de una expresión (en Fase 5). En el archivo se ve ... F.length(... ) \> 0. Ese backslash antes de > no tiene sentido en Python y puede causarte error de sintaxis dependiendo de cómo llegó el archivo. (Usar > 0 a secas).












# -------------------------
    # CLASES_PRODUCTO (LISTAGG)
    # -------------------------
    classes_uniq = PN_enriched.dropDuplicates([
        "ROW_ID_CLIENTE","TIPO_DE_DOCUMENTO_COD","NUMERO_DE_DOCUMENTO","NOMBRE_CLIENTE",
        "COD_DIVISION_GERENTE","NOMBRE_GERENTE","CODIGO_SAP_GERENTE",
        "SEGMENTO","TIPO_ATENCION","DIRECCION","CIUDAD","CLASE_PRODUCTO"
    ])

    classes_agg_client = (
        classes_uniq
        .groupBy(
            "ROW_ID_CLIENTE","TIPO_DE_DOCUMENTO_COD","NUMERO_DE_DOCUMENTO","NOMBRE_CLIENTE",
            "COD_DIVISION_GERENTE","NOMBRE_GERENTE","CODIGO_SAP_GERENTE",
            "SEGMENTO","TIPO_ATENCION","DIRECCION","CIUDAD"
        )
        .agg(
            F.array_join(
                F.array_sort(F.array_distinct(F.collect_list("CLASE_PRODUCTO"))),
                F.lit(", ")
            ).alias("CLASES_PRODUCTO")
        )
    )

    PN_df = (
        classes_agg_client.alias("c")
        .join(phones_fmt.alias("p"), F.col("p.ORG_ID") == F.col("c.ROW_ID_CLIENTE"), "left")
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
            F.col("p.TELEFONOS").alias("TELEFONOS"),
            F.col("c.DIRECCION").alias("DIRECCION"),
            F.col("c.CIUDAD").alias("CIUDAD")
        )
        .orderBy(F.col("CLIENTE").asc())
    )

    # ============================================================
    # CONSENTIMIENTOS UNIFICADOS: LEY 1581 + LEY 2300 (Formulario)
    # ============================================================

    # ---- LECTURA 1581 ----
    query_MDM1581 = """
        SELECT
            V.CONTENT AS CONTACTO,          -- canal equivalente en 1581
            G.REF_NUM AS NUM_ID_CLIENTE,    -- documento
            B.AGREE_IND,                    -- 0=no, 1=sí, 2=SC
            B.CREATE_DATE AS FECHA_DILIG
        FROM ADMMDM.CONTACT CT
        INNER JOIN ADMMDM.IDENTIFIER G       ON CT.CONT_ID = G.CONT_ID
        INNER JOIN ADMMDM.CONSENT B          ON B.CONSENT_OWNER_ID = G.CONT_ID
        INNER JOIN ADMMDM.PROCESSINGPURPOSE P ON B.PROC_PURP_ID = P.PROC_PURP_ID
        INNER JOIN ADMMDM.CONSENTREGULATION CR ON B.PROC_PURP_ID = CR.PROC_PURP_ID 
        INNER JOIN ADMMDM.PROCESSINGACTIVITY PA ON B.PROC_PURP_ID = PA.PROC_PURP_ID
        INNER JOIN ADMMDM.PROCPURPTENANTASSOC PPTA ON B.PROC_PURP_ID = PPTA.PROC_PURP_ID
        LEFT  JOIN ADMMDM.CONSENTPROVISION  V ON B.CONSENT_ID = V.CONSENT_ID
        WHERE 1 = 1
          AND P.PROC_PURP_TP_CD = '1000002'    -- Comercial
          AND REGULATION_TP_CD  = '1000001'    -- Ley 1581
          AND PROC_ACT_TP_CD    = '1000001'
          AND TENANT_TP_CD      = '1000007'
          AND B.END_DT IS NULL
          AND CT.INACTIVATED_DT IS NULL
          AND G.END_DT IS NULL
    """.strip()

    MDM1581 = glueContext.create_dynamic_frame_from_options(
        connection_type="redshift",
        connection_options={
            "connectionName": connection_name,
            "query": query_MDM1581,
            "redshiftTmpDir": temp_dir,
            "useConnectionProperties": True,
        },
    ).toDF()
    
    
    
    
    MDM1581 = (
        MDM1581
        .withColumn("doc_join",
            F.regexp_replace(F.col("NUM_ID_CLIENTE").cast("string"), r"\s+", "")
        )
        .withColumn("doc_join", F.regexp_replace(F.col("doc_join"), r"^0+", ""))
        .withColumn("FECHA_DILIG_STR", F.col("FECHA_DILIG").cast("string"))
        .withColumn(
            "ts_1581",
            F.coalesce(
                F.to_timestamp("FECHA_DILIG_STR", "dd/MM/yy hh:mm:ss,SSSSSSSSS a"),
                F.to_timestamp("FECHA_DILIG_STR", "dd/MM/yy hh:mm:ss a"),
                F.to_timestamp("FECHA_DILIG_STR", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp("FECHA_DILIG_STR")
            )
        )
        # 0 = NO contacto; 1/2 = permitido/otra categoría
        .withColumn("agree_1581",
            F.when(F.col("AGREE_IND").cast("int") == 0, F.lit(0)).otherwise(F.lit(1))
        )
    )

    w1581 = Window.partitionBy("doc_join").orderBy(F.col("ts_1581").desc_nulls_last())
    MDM1581_latest = (
        MDM1581
        .withColumn("rn", F.row_number().over(w1581))
        .filter(F.col("rn") == 1)
        .select(
            F.lit("1581").alias("ley"),
            "doc_join",
            F.col("agree_1581").alias("agree"),
            F.col("ts_1581").alias("ts"),
            # Canal unificado: 1581 usa CONTACTO
            F.col("CONTACTO").alias("canal_contacto")
        )
    )

    # ---- LECTURA 2300 ----
    query_LEY2300 = """
        SELECT
            I.ID_TP_CD                     AS TIPO_IDENTIFICACION,
            I.REF_NUM                      AS NUMERO_DE_IDENTIFICACION,
            C.CREATE_DATE                  AS FECHA_DE_INICIO,
            CAST(C.AGREE_IND AS VARCHAR2(19)) AS AGREEIND,
            C.END_DT                       AS FECHA_DE_FIN,
            NVL(V.CONTENT,'-')             AS CANAL_DE_CONTACTO,
            NVL(C.AUTH_CHNL_TP_CD,'1000000') AS TIPO_CANAL
        FROM ADMMDM.CONTACT CT
        INNER JOIN ADMMDM.IDENTIFIER I      ON CT.CONT_ID = I.CONT_ID
        INNER JOIN ADMMDM.CONSENT     C     ON I.CONT_ID = C.CONSENT_OWNER_ID
        INNER JOIN ADMMDM.CONSENTPROVISION V ON C.CONSENT_ID = V.CONSENT_ID
        INNER JOIN ADMMDM.PROCESSINGPURPOSE P ON C.PROC_PURP_ID = P.PROC_PURP_ID
        INNER JOIN ADMMDM.CONSENTREGULATION CR ON C.PROC_PURP_ID = CR.PROC_PURP_ID 
        INNER JOIN ADMMDM.PROCESSINGACTIVITY PA ON C.PROC_PURP_ID = PA.PROC_PURP_ID
        INNER JOIN ADMMDM.PROCPURPTENANTASSOC PPTA ON C.PROC_PURP_ID = PPTA.PROC_PURP_ID
        WHERE 1 = 1
          AND P.PROC_PURP_TP_CD = '1000002'   -- Comercial
          AND REGULATION_TP_CD  = '1000003'   -- Ley 2300
          AND PROC_ACT_TP_CD    = '1000001'
          AND TENANT_TP_CD      = '1000007'
          AND C.END_DT IS NULL
          AND CT.INACTIVATED_DT IS NULL 
          AND I.END_DT IS NULL
    """.strip()

    LEY2300 = glueContext.create_dynamic_frame_from_options(
        connection_type="redshift",
        connection_options={
            "connectionName": connection_name,
            "query": query_LEY2300,
            "redshiftTmpDir": temp_dir,
            "useConnectionProperties": True,
        },
    ).toDF()

    LEY2300 = (
        LEY2300
        .withColumn("doc_join",
            F.regexp_replace(F.col("NUMERO_DE_IDENTIFICACION").cast("string"), r"\s+", "")
        )
        .withColumn("doc_join", F.regexp_replace(F.col("doc_join"), r"^0+", ""))
        .withColumn("FECHA_INICIO_STR", F.col("FECHA_DE_INICIO").cast("string"))
        .withColumn(
            "ts_2300",
            F.coalesce(
                F.to_timestamp("FECHA_INICIO_STR", "dd/MM/yy hh:mm:ss,SSSSSSSSS a"),
                F.to_timestamp("FECHA_INICIO_STR", "dd/MM/yy hh:mm:ss a"),
                F.to_timestamp("FECHA_INICIO_STR", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp("FECHA_INICIO_STR")
            )
        )
        .withColumn("agree_2300",
            F.when(F.trim(F.col("AGREEIND")) == F.lit("0"), F.lit(0))
             .when(F.trim(F.col("AGREEIND")) == F.lit("1"), F.lit(1))
             .otherwise(F.col("AGREEIND").cast("int"))
        )
    )

    w2300 = Window.partitionBy("doc_join").orderBy(F.col("ts_2300").desc_nulls_last())
    LEY2300_latest = (
        LEY2300
        .withColumn("rn", F.row_number().over(w2300))
        .filter(F.col("rn") == 1)
        .select(
            F.lit("2300").alias("ley"),
            "doc_join",
            F.col("agree_2300").alias("agree"),
            F.col("ts_2300").alias("ts"),
            # Canal unificado: 2300 usa CANAL_DE_CONTACTO
            F.col("CANAL_DE_CONTACTO").alias("canal_contacto")
        )
    )

    # ---- UNION de leyes y elección de la MÁS RECIENTE por documento ----
    consent_candidates = MDM1581_latest.unionByName(LEY2300_latest)

    # Empate por fecha (opcional): priorizar 2300 sobre 1581 si el timestamp es idéntico
    consent_rank = Window.partitionBy("doc_join").orderBy(
        F.col("ts").desc_nulls_last(),
        F.when(F.col("ley") == "2300", F.lit(1)).otherwise(F.lit(0)).desc()
    )

    consent_latest = (
        consent_candidates
        .withColumn("rn_all", F.row_number().over(consent_rank))
        .filter(F.col("rn_all") == 1)
        .select("doc_join", "ley", "agree", "ts", "canal_contacto")
    )

    # ---- EXCLUSIÓN: si el consentimiento más reciente tiene agree=0, NO se incluye el cliente ----
    consent_excluir = consent_latest.filter(F.col("agree") == 0).select("doc_join").distinct()
    consent_permitidos = consent_latest.filter(F.col("agree") != 0).select("doc_join", "canal_contacto")

    # ---- Clave en PN y depuración final ----
    PN_df = (
        PN_df
        .withColumn("doc_join",
            F.regexp_replace(F.col("IDENT").cast("string"), r"\s+", "")
        )
        .withColumn("doc_join", F.regexp_replace(F.col("doc_join"), r"^0+", ""))
    )

    # 1) Eliminar clientes que NO permiten contacto según su consentimiento más reciente (1581 o 2300)
    PN_dep = PN_df.join(consent_excluir, on="doc_join", how="left_anti")

    # 2) Añadir el CANAL_DE_CONTACTO (según la ley ganadora por fecha)
    PN_final = PN_dep.alias("pn").join(
        consent_permitidos.alias("cs"),
        on="doc_join", how="left"
    )

    # 3) Selección final (PN + canal de contacto)
    df_final = (
        PN_final
        .select(
            "pn.SEGMENTO",
            F.col("pn.`TIPO DE DOCUMENTO`").alias("TIPO DE DOCUMENTO"),
            "pn.IDENT",
            "pn.CLIENTE",
            "pn.CAPA",
            F.col("pn.`CODIGO ZONAL`").alias("CODIGO ZONAL"),
            F.col("pn.`CODIGO SAP GERENTE`").alias("CODIGO SAP GERENTE"),
            "pn.NOMBRE_GERENTE",
            "pn.PRODUCTO",
            "pn.TELEFONOS",
            "pn.DIRECCION",
            "pn.CIUDAD",
            F.col("cs.canal_contacto").alias("CANAL_DE_CONTACTO")
        )
    )

    # -------------------------
    # ESCRITURA S3
    # -------------------------
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")
    final_key = f"PN_LEYES_1581_2300_{now_str}.csv"
    bucket = f"cdata-bocc-{var_env}-output"
    prefix = "aplicaciones/apl_cdata/salida/qualtrics/"

    (
        df_final.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .csv(f"s3://{bucket}/{prefix}{final_key}_tmp")
    )
    print(f"Archivo generado en s3://{bucket}/{prefix}{final_key}_tmp")

    # -------------------------
    # (Opcional) Validaciones rápidas
    # -------------------------
    try:
        print("PN_base:", PN_base.count())
        print("PN_enriched:", PN_enriched.count())
        print("phones_raw:", phones_raw.count(), "phones_fmt:", phones_fmt.count())
        print("classes_agg_client:", classes_agg_client.count())
        print("PN_df (con doc_join previo a depurar):", PN_df.count())

        print("MDM1581_latest:", MDM1581_latest.count())
        print("LEY2300_latest:", LEY2300_latest.count())
        print("consent_latest:", consent_latest.count())
        print("consent_excluir:", consent_excluir.count())

        print("Final:", df_final.count())
        df_final.limit(10).show(truncate=False)
    except Exception as e:
        print(f"[WARN] Validaciones no ejecutadas: {e}")
