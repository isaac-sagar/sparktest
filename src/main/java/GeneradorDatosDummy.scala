import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.UUID

object GeneradorDatosDummy extends App {

  private val carpeta = "TransferDetail"
  private val ruta = ""
  private val pathToOriginalParquetFile = ruta + carpeta +"\\part-00000-1b729b5c-5d12-4bf0-8186-70fb4fa2142f-c000.snappy.parquet"
  private val pathToDestinationFiles = ruta + carpeta

  val conf = new SparkConf().setMaster("local[*]").setAppName("my app")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()

  createParquetFromData

  def createParquetFromData() : Unit = {
    val data = getDataForContractRelationshipLcl()
    val customSchema = getCustomSchemaForContractRelationshipLcl()

    writeInParquetFile(data, customSchema)
  }

  def checkParquetCreated() : Unit = {
    val originalParquetDf = spark.read.format("parquet").option("header", "true").load(pathToOriginalParquetFile)
    originalParquetDf.printSchema()
    originalParquetDf.show(false)
  }

  def getCustomSchemaForContractInterventGlobal() : StructType = {
    StructType(Array(
      StructField("gf_local_contract_number_id", StringType, true),
      StructField("gf_rel_lcl_contract_number_id", StringType, true),
      StructField("gf_contracts_relationship_type", StringType, true),
      StructField("gf_contracts_rel_start_date", DateType, true),
      StructField("gf_contracts_rel_end_date", DateType, true),
      StructField("gf_audit_operuser_id", StringType, true),
      StructField("gf_operational_audit_date", TimestampType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true),
      StructField("g_record_update_type", StringType, true)
    ))
  }

  def getDataForContractInterventGlobal() : Seq[Row] = {
    Seq(
      Row("ES007411577340402244","ES007447729157126001","CPH",createDate("26/10/2020"),createDate("27/05/1857"),"MP9CCARG",createTimestamp("2020-10-26T00:00:00.000Z"), createTimestamp("2020-10-29T00:00:00.000Z"), createDate("23/10/2020"), "DELETED"),
      Row("ES007411577340402245","ES007447729157126002","CPH",createDate("26/10/2020"),createDate("29/05/1857"),"MP9CCARG",createTimestamp("2020-10-26T00:00:00.000Z"), createTimestamp("2020-10-29T00:00:00.000Z"), createDate("23/10/2020"), "DELETED"),
    )
  }

  def getCustomSchemaForContractRelationshipLcl() : StructType = {
    StructType(Array(
      StructField("gf_lcl_main_contract_number_id", StringType, true),
      StructField("gf_rel_lcl_contract_number_id", StringType, true),
      StructField("gf_contracts_relationship_type", StringType, true),
      StructField("gf_contracts_rel_start_date", DateType, true),
      StructField("gf_contracts_rel_end_date", DateType, true),
      StructField("gf_audit_operuser_id", StringType, true),
      StructField("gf_operational_audit_date", TimestampType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true),
      StructField("g_record_update_type", StringType, true)
    ))
  }

  def getDataForContractRelationshipLcl() : Seq[Row] = {
    Seq(
      Row("ES007411577340402244","ES007447729157126001","CPH",createDate("26/10/2020"),createDate("27/05/1857"),"MP9CCARG",createTimestamp("2020-10-26T00:00:00.000Z"), createTimestamp("2020-10-29T00:00:00.000Z"), createDate("23/10/2020"), "DELETED"),
      Row("ES007411577340402245","ES007447729157126002","CPH",createDate("26/10/2020"),createDate("29/05/1857"),"MP9CCARG",createTimestamp("2020-10-26T00:00:00.000Z"), createTimestamp("2020-10-29T00:00:00.000Z"), createDate("23/10/2020"), "DELETED"),
       )
  }

  def getDataForFundBalancesLcl() : Seq[Row] = {
    //gf_cutoff_date,gf_local_contract_number_id,g_currency_id,gf_participations_value_amount,gf_cont_participations_number,gf_user_audit_id,gf_audit_date,g_record_update_type
    Seq(
      Row(createDate("18/03/2021"),randomNumericString(80),"EUR",randomDecimal(20,6),randomDecimal(20,6),randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(null,randomNumericString(80),"EUR",randomDecimal(20,6),randomDecimal(20,6),randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"),randomNumericString(80),null,randomDecimal(20,6),randomDecimal(20,6),randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"),randomNumericString(80),"EUR",null,randomDecimal(20,6),randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"),randomNumericString(80),"EUR",randomDecimal(20,6),null,randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"),randomNumericString(80),"EUR",randomDecimal(20,6),randomDecimal(20,6),null, createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"),randomNumericString(80),"EUR",randomDecimal(20,6),randomDecimal(20,6),randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), null),
      Row(createDate("18/03/2021"),randomNumericString(80),"EUR",randomDecimal(20,6),randomDecimal(20,6),randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "FAIL")
    )
  }

  def getCustomSchemaForFundBalancesLcl() : StructType = {
    StructType(Array(
      StructField("gf_cutoff_date", DateType, true),
      StructField("gf_local_contract_number_id", StringType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_participations_value_amount", DecimalType(26,6), true),
      StructField("gf_cont_participations_number", DecimalType(26,6), true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("g_record_update_type", StringType, true)
    ))
  }


  def getDataForFundConditionsLcl() : Seq[Row] = {
    //gf_cutoff_date,gf_local_contract_number_id,g_currency_id,gf_ini_pdic_cntrb_date,gf_final_pdic_cntrb_date,g_pdic_cntrb_status_type,gf_first_contribution_amount,g_periodic_contrib_type,gf_user_audit_id,gf_audit_date,g_record_update_type
    Seq(
      Row(createDate("18/03/2021"), randomNumericString(80), "EUR", createDate("18/03/2021"), createDate("18/03/2021"), "NOPLAN", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), null, "EUR", createDate("18/03/2021"), createDate("18/03/2021"), "NOPLAN", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), null, createDate("18/03/2021"), createDate("18/03/2021"), "NOPLAN", null,randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), null, createDate("18/03/2021"), createDate("18/03/2021"), "NOPLAN", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), "EUR", createDate("18/03/8888"), createDate("18/03/8888"), "NOPLAN", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), "EUR", createDate("18/03/8888"), createDate("18/03/8888"), "INFORCE", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), "EUR", createDate("18/03/2021"), createDate("18/03/2021"), "JAAARL", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), "EUR", createDate("18/03/2021"), createDate("18/03/2021"), "NOPLAN", randomDecimal(20,6),randomNumericString(80), null, createTimestamp("2021-03-18T00:00:00.000Z"), "INSERTED"),
      Row(createDate("18/03/2021"), randomNumericString(80), "EUR", createDate("18/03/2021"), createDate("18/03/2021"), "NOPLAN", randomDecimal(20,6),randomNumericString(80), randomNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), null),
    )
  }

  def getCustomSchemaForFundConditionsLcl() : StructType = {
    StructType(Array(
      StructField("gf_cutoff_date", DateType, true),
      StructField("gf_local_contract_number_id", StringType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_ini_pdic_cntrb_date", DateType, true),
      StructField("gf_final_pdic_cntrb_date", DateType, true),
      StructField("g_pdic_cntrb_status_type", StringType, true),
      StructField("gf_first_contribution_amount", DecimalType(26,6), true),
      StructField("g_periodic_contrib_type", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("g_record_update_type", StringType, true)
    ))
  }

  def getDataForAccountBalances() : Seq[Row] = {


    Seq(
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021")),
      Row(randomNumericString(40), "EUR", randomDecimal(10,2), randomDecimal(6, 2), randomAlphaNumericString(50), createTimestamp("2021-03-18T00:00:00.000Z"), createDate("18/03/2021"))
    )
  }

  def getCustomSchemaForAccountBalances() : StructType = {
    StructType(Array(
      StructField("g_contract_id", StringType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_available_amount", DecimalType(26,6), true),
      StructField("gf_accounting_balance_amount", DecimalType(26,6), true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true)
    ))
  }

  def getDataForSetPayments() : Seq[Row] = {
    //g_movement_id,gf_set_trans_order_details_id,gf_trans_order_details_id,g_movement_type,gf_local_contract_number_id,g_entity_id,g_branch_id,gf_account_id,gf_movement_id,gf_operation_date,gf_acst_trans_legend_type,gf_movement_type,gf_commercial_product_grp_id,g_owner_account_type,g_account_ref_move_id,gf_local_account_contract_id,gf_related_account_id,gf_related_mov_pymt_id,gf_invest_seq_id,gf_oper_nature_type,gf_clsfn_operation_type,gf_ip_id,gf_commerce_id,gf_atm_id,gf_contract_id,g_pan_id,gf_channel_id,gf_trans_online_batch_id,gf_rec_update_proc_name,gf_opern_audit_insert_date,gf_operational_audit_date,gf_audit_operuser_id,gf_user_audit_id,gf_audit_date,gf_cutoff_date
    Seq(Row("MX00740206570181393448000087586",randomAlphaNumericString(36),randomAlphaNumericString(36),"setPayTr",randomAlphaNumericString(80),randomAlphaNumericString(6),randomAlphaNumericString(10),randomAlphaNumericString(40),randomDecimal(12, -1),createDate("01/06/2020"),randomAlphaNumericString(3),randomAlphaNumericString(2),randomAlphaNumericString(4),"internal",randomAlphaNumericString(70),randomAlphaNumericString(80),randomAlphaNumericString(40),randomDecimal(12, -1),randomDecimal(13, -1),randomAlphaNumericString(2),randomAlphaNumericString(2),randomAlphaNumericString(39),randomAlphaNumericString(26),randomAlphaNumericString(8),randomAlphaNumericString(49),randomAlphaNumericString(20),randomAlphaNumericString(8),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087587",randomAlphaNumericString(36),randomAlphaNumericString(36),"setPayEm",randomAlphaNumericString(80),randomAlphaNumericString(6),randomAlphaNumericString(10),randomAlphaNumericString(40),randomDecimal(12, -1),createDate("01/06/2020"),randomAlphaNumericString(3),randomAlphaNumericString(2),randomAlphaNumericString(4),"internal",randomAlphaNumericString(70),randomAlphaNumericString(80),randomAlphaNumericString(40),randomDecimal(12, -1),randomDecimal(13, -1),randomAlphaNumericString(2),randomAlphaNumericString(2),randomAlphaNumericString(39),randomAlphaNumericString(26),randomAlphaNumericString(8),randomAlphaNumericString(49),randomAlphaNumericString(20),randomAlphaNumericString(8),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087588",randomAlphaNumericString(36),randomAlphaNumericString(36),"setPayRe",randomAlphaNumericString(80),randomAlphaNumericString(6),randomAlphaNumericString(10),randomAlphaNumericString(40),randomDecimal(12, -1),createDate("01/06/2020"),randomAlphaNumericString(3),randomAlphaNumericString(2),randomAlphaNumericString(4),"external",randomAlphaNumericString(70),randomAlphaNumericString(80),randomAlphaNumericString(40),randomDecimal(12, -1),randomDecimal(13, -1),randomAlphaNumericString(2),randomAlphaNumericString(2),randomAlphaNumericString(39),randomAlphaNumericString(26),randomAlphaNumericString(8),randomAlphaNumericString(49),randomAlphaNumericString(20),randomAlphaNumericString(8),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087589",randomAlphaNumericString(36),randomAlphaNumericString(36),"setPayTj",randomAlphaNumericString(80),randomAlphaNumericString(6),randomAlphaNumericString(10),randomAlphaNumericString(40),randomDecimal(12, -1),createDate("01/06/2020"),randomAlphaNumericString(3),randomAlphaNumericString(2),randomAlphaNumericString(4),"external",randomAlphaNumericString(70),randomAlphaNumericString(80),randomAlphaNumericString(40),randomDecimal(12, -1),randomDecimal(13, -1),randomAlphaNumericString(2),randomAlphaNumericString(2),randomAlphaNumericString(39),randomAlphaNumericString(26),randomAlphaNumericString(8),randomAlphaNumericString(49),randomAlphaNumericString(20),randomAlphaNumericString(8),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087590",randomAlphaNumericString(36),randomAlphaNumericString(36),"setPayTb",randomAlphaNumericString(80),randomAlphaNumericString(6),randomAlphaNumericString(10),randomAlphaNumericString(40),randomDecimal(12, -1),createDate("01/06/2020"),randomAlphaNumericString(3),randomAlphaNumericString(2),randomAlphaNumericString(4),"internal",randomAlphaNumericString(70),randomAlphaNumericString(80),randomAlphaNumericString(40),randomDecimal(12, -1),randomDecimal(13, -1),randomAlphaNumericString(2),randomAlphaNumericString(2),randomAlphaNumericString(39),randomAlphaNumericString(26),randomAlphaNumericString(8),randomAlphaNumericString(49),randomAlphaNumericString(20),randomAlphaNumericString(8),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087591",randomAlphaNumericString(36),randomAlphaNumericString(36),"setPaySt",randomAlphaNumericString(80),randomAlphaNumericString(6),randomAlphaNumericString(10),randomAlphaNumericString(40),randomDecimal(12, -1),createDate("01/06/2020"),randomAlphaNumericString(3),randomAlphaNumericString(2),randomAlphaNumericString(4),"external",randomAlphaNumericString(70),randomAlphaNumericString(80),randomAlphaNumericString(40),randomDecimal(12, -1),randomDecimal(13, -1),randomAlphaNumericString(2),randomAlphaNumericString(2),randomAlphaNumericString(39),randomAlphaNumericString(26),randomAlphaNumericString(8),randomAlphaNumericString(49),randomAlphaNumericString(20),randomAlphaNumericString(8),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),randomAlphaNumericString(8),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020"))
    )
  }

  def getDataForLoanDetail() : Seq[Row] = {
    //g_movement_id,g_detail_loan_type,g_account_ref_move_id,g_banking_operation_id,gf_local_account_contract_id,gf_detail_loan_amount,g_detail_loan_currency_id,gf_operation_date,gf_opern_audit_insert_date,gf_operational_audit_date,gf_audit_operuser_id,gf_user_audit_id,gf_audit_date,gf_cutoff_date
    Seq(Row("MX00740206570181393448000087586","capital","GACCOUNTREFMOVES000001","BANKINGOPID0001","MX007406570181393448",new java.math.BigDecimal("12345678912345678954.321568"),"ABC",createDate("01/06/2020"),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),"BG7CCPP0","MX_DAAS",createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087587","capAdj","GACCOUNTREFMOVES000002","BANKINGOPID0002","MX007406570181393449",new java.math.BigDecimal("12345678912345678955.321569"),"ABC",createDate("01/06/2020"),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),"BG7CCPP1","MX_DAAS",createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087588","interest","GACCOUNTREFMOVES000003","BANKINGOPID0003","MX007406570181393450",new java.math.BigDecimal("12345678912345678956.321570"),"ABC",createDate("01/06/2020"),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),"BG7CCPP2","MX_DAAS",createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row("MX00740206570181393448000087589","feePaid","GACCOUNTREFMOVES000004","BANKINGOPID0004","MX007406570181393451",new java.math.BigDecimal("12345678912345678957.321571"),"ABC",createDate("01/06/2020"),createTimestamp("2020-06-01T12:27:00.000Z"),createTimestamp("2020-06-01T12:27:38.762Z"),"BG7CCPP3","MX_DAAS",createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020"))
    )
  }

  def getDataForTransfer() : Seq[Row] = {
    //g_movement_id,g_country_id,g_entity_id,g_local_movement_id_type,gf_local_movement_id,g_movement_type,g_account_ref_move_id,g_banking_operation_id,gf_local_account_contract_id,gf_transfer_concept_id,gf_payment_channel_type,gf_operation_date,gf_ori_format_account_type,gf_ori_account_number_id,gf_ori_doc_type_id,gf_ori_personal_id,g_ordering_country_id,gf_payment_originator_bic_id,gf_ori_contract_entity_name,gf_trans_done_person_full_name,gf_ori_phone_id,gf_beneficiary_doc_type_id,gf_beneficiary_doc_number_id,g_benef_trans_country_id,gf_beneficiary_name,gf_benef_phone_id,gf_benef_account_format_type,gf_beneficiary_account_id,g_swift_id,gf_benef_contract_entity_name,g_foreign_transfer_type,g_transfer_detail_type,gf_debit_amount,g_debit_currency_id,gf_transfer_debit_date,gf_exchange_rate_agreed_per,gf_exchange_rate_agreed1_per,gf_benef_transfer_amount,g_credit_currency_id,gf_transfer_payment_date,g_frequency_transaction_type,gf_freq_trans_period_number,gf_period_creation_date,gf_period_end_date,gf_period_next_payment_date,g_period_valid_type,gf_payment_status_type,gf_transaction_reference_id,gf_movement_external_id,gf_nominal_amount,g_nominal_currency_id,gf_mt103_swift_id,gf_total_fees_amount,g_total_fees_currency_id,gf_transaction_comments_desc,gf_transfer_type,gf_value_date,gf_recurrent_transfer_ref_id,gf_fee_iban_id,gf_transfer_payment_amount,g_transfer_pay_currency_id,g_expenses_key_type,g_swift_message_ind_type,gf_basic_service_id,gf_transfer_priority_type,gf_channel_id,gf_channel_means_id,gf_means_service_id,gf_bizum_payer_alias_desc,gf_bizum_benef_alias_desc,gf_commerce_id,gf_commerce_name,gf_julian_date_number,gf_transfer_hms_date,gf_transfer_sequence_id,g_ordering_corresp_entity_id,gf_ord_corresp_entity_name,g_benef_corresp_entity_id,gf_benef_corresp_entity_name,gf_benef_corresp_account_id,gf_opern_audit_insert_date,gf_operational_audit_date,gf_audit_operuser_id,gf_user_audit_id,gf_audit_date,gf_cutoff_date
    Seq(Row("GMOVEMENTID740206570181393448000087586",randomAlphaNumericString(2), randomAlphaNumericString(6), randomAlphaNumericString(2), randomAlphaNumericString(62), "transfer", randomAlphaNumericString(70), randomAlphaNumericString(40), randomAlphaNumericString(80), randomAlphaNumericString(5), randomAlphaNumericString(8), createDate("02/06/2020"), randomAlphaNumericString(8), randomAlphaNumericString(80), randomAlphaNumericString(8), randomAlphaNumericString(35), randomAlphaNumericString(2), randomAlphaNumericString(20), randomAlphaNumericString(40), randomAlphaNumericString(80), randomAlphaNumericString(21), randomAlphaNumericString(8), randomAlphaNumericString(35), randomAlphaNumericString(2), randomAlphaNumericString(80), randomAlphaNumericString(21), randomAlphaNumericString(8), randomAlphaNumericString(80), randomAlphaNumericString(11), randomAlphaNumericString(70), "national", "transDeb", randomDecimal(20,6), randomAlphaNumericString(3), createDate("02/06/2020"), randomDecimal(3,9), randomDecimal(3,9), randomDecimal(20,6), randomAlphaNumericString(3), createDate("02/06/2020"), "months", randomDecimal(4, -1), createDate("02/06/2020"), createDate("02/06/2020"), createDate("02/06/2020"), "true", randomAlphaNumericString(8), randomAlphaNumericString(36), randomAlphaNumericString(36), randomDecimal(20,6), randomAlphaNumericString(3), randomAlphaNumericString(12), randomDecimal(20,6), randomAlphaNumericString(3), randomAlphaNumericString(140), randomAlphaNumericString(8), createDate("02/06/2020"), randomAlphaNumericString(12), randomAlphaNumericString(50), randomDecimal(20,6), randomAlphaNumericString(3), "SHA", "Y", randomDecimal(4, -1), randomAlphaNumericString(1), randomAlphaNumericString(8), randomAlphaNumericString(2), randomAlphaNumericString(4), randomAlphaNumericString(40), randomAlphaNumericString(40), randomAlphaNumericString(26), randomAlphaNumericString(40), randomDecimal(7, -1), createTimestamp("2020-06-02T00:00:00.000Z"), randomAlphaNumericString(6), randomAlphaNumericString(6), randomAlphaNumericString(40), randomAlphaNumericString(6), randomAlphaNumericString(40), randomAlphaNumericString(80), createTimestamp("2020-06-02T00:00:00.000Z"), createTimestamp("2020-06-02T00:00:00.000Z"), randomAlphaNumericString(8), randomAlphaNumericString(50), createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020")),
      Row("GMOVEMENTID740206570181393448000087587",randomAlphaNumericString(2), randomAlphaNumericString(6), randomAlphaNumericString(2), randomAlphaNumericString(62), "transfer", randomAlphaNumericString(70), randomAlphaNumericString(40), randomAlphaNumericString(80), randomAlphaNumericString(5), randomAlphaNumericString(8), createDate("02/06/2020"), randomAlphaNumericString(8), randomAlphaNumericString(80), randomAlphaNumericString(8), randomAlphaNumericString(35), randomAlphaNumericString(2), randomAlphaNumericString(20), randomAlphaNumericString(40), randomAlphaNumericString(80), randomAlphaNumericString(21), randomAlphaNumericString(8), randomAlphaNumericString(35), randomAlphaNumericString(2), randomAlphaNumericString(80), randomAlphaNumericString(21), randomAlphaNumericString(8), randomAlphaNumericString(80), randomAlphaNumericString(11), randomAlphaNumericString(70), "internac", "transCrd", randomDecimal(20,6), randomAlphaNumericString(3), createDate("02/06/2020"), randomDecimal(3,9), randomDecimal(3,9), randomDecimal(20,6), randomAlphaNumericString(3), createDate("02/06/2020"), "punctual", randomDecimal(4, -1), createDate("02/06/2020"), createDate("02/06/2020"), createDate("02/06/2020"), "false", randomAlphaNumericString(8), randomAlphaNumericString(36), randomAlphaNumericString(36), randomDecimal(20,6), randomAlphaNumericString(3), randomAlphaNumericString(12), randomDecimal(20,6), randomAlphaNumericString(3), randomAlphaNumericString(140), randomAlphaNumericString(8), createDate("02/06/2020"), randomAlphaNumericString(12), randomAlphaNumericString(50), randomDecimal(20,6), randomAlphaNumericString(3), "CRE", "N", randomDecimal(4, -1), randomAlphaNumericString(1), randomAlphaNumericString(8), randomAlphaNumericString(2), randomAlphaNumericString(4), randomAlphaNumericString(40), randomAlphaNumericString(40), randomAlphaNumericString(26), randomAlphaNumericString(40), randomDecimal(7, -1), createTimestamp("2020-06-02T00:00:00.000Z"), randomAlphaNumericString(6), randomAlphaNumericString(6), randomAlphaNumericString(40), randomAlphaNumericString(6), randomAlphaNumericString(40), randomAlphaNumericString(80), createTimestamp("2020-06-02T00:00:00.000Z"), createTimestamp("2020-06-02T00:00:00.000Z"), randomAlphaNumericString(8), randomAlphaNumericString(50), createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020"))
    )
  }

  def getDataForTransferDetail() : Seq[Row] = {
    //g_movement_id,gf_transfer_move_detail_id,gf_ref_transfer_move_detail_id,g_movement_type,gf_concept_id,g_banking_operation_id,gf_transfer_detail_amount,g_transfer_detail_ccy_id,gf_exchange_rate_agreed_per,gf_tax_rate_per,gf_tax_type,gf_operation_date,gf_opern_audit_insert_date,gf_operational_audit_date,gf_audit_operuser_id,gf_user_audit_id,gf_audit_date,gf_cutoff_date
    Seq(Row("GMOVEMENTID740206570181393448000087586",randomAlphaNumericString(36), randomAlphaNumericString(36), "fees", "C;01", randomAlphaNumericString(40), randomDecimal(20,6), randomAlphaNumericString(3), randomDecimal(3, 9), randomDecimal(16,10), randomAlphaNumericString(2), createDate("02/06/2020"), createTimestamp("2020-06-02T00:00:00.000Z"), createTimestamp("2020-06-02T00:00:00.000Z"), randomAlphaNumericString(8), randomAlphaNumericString(50), createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020")),
      Row("GMOVEMENTID740206570181393448000087587",randomAlphaNumericString(36), randomAlphaNumericString(36), "taxes", "C;02", randomAlphaNumericString(40), randomDecimal(20, 6), randomAlphaNumericString(3), randomDecimal(3, 9), randomDecimal(16,10), randomAlphaNumericString(2), createDate("02/06/2020"), createTimestamp("2020-06-02T00:00:00.000Z"), createTimestamp("2020-06-02T00:00:00.000Z"), randomAlphaNumericString(8), randomAlphaNumericString(50), createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020"))
    )
  }

  def getDataForCardInstlPlan() : Seq[Row] = {
    //g_contract_id,gf_installment_maturity_date,g_currency_id,gf_cap_amortiz_instlmnt_amount,gf_int_amort_instlmnt_amount,gf_total_installment_amount,gf_instlmnt_payment_ind_type,gf_user_audit_id,gf_audit_date,gf_cutoff_date
    Seq(
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),null),
      Row(null, createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), null,"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),null,randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),null,"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Z", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", null,createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020")),
      Row(randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),null, randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),createDate("02/06/2020"))
    )
  }

  def getDataForCardInstlPlanLcl() : Seq[Row] = {
    //gf_cutoff_date,gf_local_contract_number_id,gf_installment_maturity_date,g_currency_id,gf_cap_amortiz_instlmnt_amount,gf_int_amort_instlmnt_amount,gf_total_installment_amount,gf_instlmnt_payment_ind_type,gf_user_audit_id,gf_audit_date,g_record_update_type
    Seq(
      Row(null,randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),null, createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), null,"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),null,randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),null,"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Z", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", null,createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),null),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),"Y", randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"JARL"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80), createDate("02/06/2020"),"USD",randomDecimal(20,6),randomDecimal(20,6),randomDecimal(20,6),null, randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED")
    )
  }


  def getDataForCardConditionsLcl() : Seq[Row] = {
    Seq(
      Row(createDate("02/06/2020"),null,"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"),"INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80),null,randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), "INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80),"USD",randomDecimal(20,6),null,createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), "INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80),"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),null,createTimestamp("2020-06-02T00:00:00.000Z"), "INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80),"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), "FAILED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80),"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), "INSERTED"),
      Row(createDate("02/06/2020"),randomAlphaNumericString(80),null,null,randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), "INSERTED")
    )
  }


  def getCustomSchemaForCardConditionsLcl() : StructType = {
    StructType(Array(
      StructField("gf_cutoff_date", DateType, true),
      StructField("gf_local_contract_number_id", StringType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_auth_limit_amount", DecimalType(26,6), true),
      StructField("g_pan_id", StringType, true),
      StructField("gf_last_settlement_date", DateType, true),
      StructField("gf_next_settlement_date", DateType, true),
      StructField("gf_interest_rate_per", DecimalType(12,9), true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("g_record_update_type", StringType, true)
    ))
  }

  def getDataForCardConditions() : Seq[Row] = {
    Seq(
      Row(null,"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020")),
      Row(randomAlphaNumericString(80),null,randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020")),
      Row(randomAlphaNumericString(80),"USD",randomDecimal(20,6),null,createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020")),
      Row(randomAlphaNumericString(80),"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),null,createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020")),
      Row(randomAlphaNumericString(80),"USD",randomDecimal(20,6),randomAlphaNumericString(20),createDate("02/06/2020"),createDate("02/06/2020"),randomDecimal(3,9),randomAlphaNumericString(50),createTimestamp("2020-06-02T00:00:00.000Z"), createDate("02/06/2020"))
    )
  }

  def getCustomSchemaForCardConditions() : StructType = {
    StructType(Array(
      StructField("g_contract_id", StringType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_auth_limit_amount", DecimalType(26,6), true),
      StructField("g_pan_id", StringType, true),
      StructField("gf_last_settlement_date", DateType, true),
      StructField("gf_next_settlement_date", DateType, true),
      StructField("gf_interest_rate_per", DecimalType(12,9), true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true),
    ))
  }

  def getCustomSchemaForCardInstlPlanLcl() : StructType = {
    StructType(Array(
      StructField("gf_cutoff_date", DateType, true),
      StructField("gf_local_contract_number_id", StringType, true),
      StructField("gf_installment_maturity_date", DateType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_cap_amortiz_instlmnt_amount", DecimalType(26,6), true),
      StructField("gf_int_amort_instlmnt_amount", DecimalType(26,6), true),
      StructField("gf_total_installment_amount", DecimalType(26,6), true),
      StructField("gf_instlmnt_payment_ind_type", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("g_record_update_type", StringType, true)
    ))
  }

  def getCustomSchemaForCardInstlPlan() : StructType = {
    StructType(Array(
      StructField("g_contract_id", StringType, true),
      StructField("gf_installment_maturity_date", DateType, true),
      StructField("g_currency_id", StringType, true),
      StructField("gf_cap_amortiz_instlmnt_amount", DecimalType(26,6), true),
      StructField("gf_int_amort_instlmnt_amount", DecimalType(26,6), true),
      StructField("gf_total_installment_amount", DecimalType(26,6), true),
      StructField("gf_instlmnt_payment_ind_type", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true)
    ))
  }

  def getCustomSchemaForSetPayments() : StructType = {
    StructType(Array(
      StructField("g_movement_id", StringType, true),
      StructField("gf_set_trans_order_details_id", StringType, true),
      StructField("gf_trans_order_details_id", StringType, true),
      StructField("g_movement_type", StringType, true),
      StructField("gf_local_contract_number_id", StringType, true),
      StructField("g_entity_id", StringType, true),
      StructField("g_branch_id", StringType, true),
      StructField("gf_account_id", StringType, true),
      StructField("gf_movement_id", DecimalType(12,0), true),
      StructField("gf_operation_date", DateType, true),
      StructField("gf_acst_trans_legend_type", StringType, true),
      StructField("gf_movement_type", StringType, true),
      StructField("gf_commercial_product_grp_id", StringType, true),
      StructField("g_owner_account_type", StringType, true),
      StructField("g_account_ref_move_id", StringType, true),
      StructField("gf_local_account_contract_id", StringType, true),
      StructField("gf_related_account_id", StringType, true),
      StructField("gf_related_mov_pymt_id", DecimalType(12,0), true),
      StructField("gf_invest_seq_id", DecimalType(13,0), true),
      StructField("gf_oper_nature_type", StringType, true),
      StructField("gf_clsfn_operation_type", StringType, true),
      StructField("gf_ip_id", StringType, true),
      StructField("gf_commerce_id", StringType, true),
      StructField("gf_atm_id", StringType, true),
      StructField("gf_contract_id", StringType, true),
      StructField("g_pan_id", StringType, true),
      StructField("gf_channel_id", StringType, true),
      StructField("gf_trans_online_batch_id", StringType, true),
      StructField("gf_rec_update_proc_name", StringType, true),
      StructField("gf_opern_audit_insert_date", TimestampType, true),
      StructField("gf_operational_audit_date", TimestampType, true),
      StructField("gf_audit_operuser_id", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true)
    ))
  }

  def getCustomSchemaForLoanDetail() : StructType = {
    StructType(Array(
      StructField("g_movement_id", StringType, true),
      StructField("g_detail_loan_type", StringType, true),
      StructField("g_account_ref_move_id", StringType, true),
      StructField("g_banking_operation_id", StringType, true),
      StructField("gf_local_account_contract_id", StringType, true),
      StructField("gf_detail_loan_amount", DecimalType(26,6), true),
      StructField("g_detail_loan_currency_id", StringType, true),
      StructField("gf_operation_date", DateType, true),
      StructField("gf_opern_audit_insert_date", TimestampType, true),
      StructField("gf_operational_audit_date", TimestampType, true),
      StructField("gf_audit_operuser_id", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true)
    ))
  }

  def getCustomSchemaForTransfer() : StructType = {
    StructType(Array(
      StructField("g_movement_id", StringType, true),
      StructField("g_country_id", StringType, true),
      StructField("g_entity_id", StringType, true),
      StructField("g_local_movement_id_type", StringType, true),
      StructField("gf_local_movement_id", StringType, true),
      StructField("g_movement_type", StringType, true),
      StructField("g_account_ref_move_id", StringType, true),
      StructField("g_banking_operation_id", StringType, true),
      StructField("gf_local_account_contract_id", StringType, true),
      StructField("gf_transfer_concept_id", StringType, true),
      StructField("gf_payment_channel_type", StringType, true),
      StructField("gf_operation_date", DateType, true),
      StructField("gf_ori_format_account_type", StringType, true),
      StructField("gf_ori_account_number_id", StringType, true),
      StructField("gf_ori_doc_type_id", StringType, true),
      StructField("gf_ori_personal_id", StringType, true),
      StructField("g_ordering_country_id", StringType, true),
      StructField("gf_payment_originator_bic_id", StringType, true),
      StructField("gf_ori_contract_entity_name", StringType, true),
      StructField("gf_trans_done_person_full_name", StringType, true),
      StructField("gf_ori_phone_id", StringType, true),
      StructField("gf_beneficiary_doc_type_id", StringType, true),
      StructField("gf_beneficiary_doc_number_id", StringType, true),
      StructField("g_benef_trans_country_id", StringType, true),
      StructField("gf_beneficiary_name", StringType, true),
      StructField("gf_benef_phone_id", StringType, true),
      StructField("gf_benef_account_format_type", StringType, true),
      StructField("gf_beneficiary_account_id", StringType, true),
      StructField("g_swift_id", StringType, true),
      StructField("gf_benef_contract_entity_name", StringType, true),
      StructField("g_foreign_transfer_type", StringType, true),
      StructField("g_transfer_detail_type", StringType, true),
      StructField("gf_debit_amount", DecimalType(26,6), true),
      StructField("g_debit_currency_id", StringType, true),
      StructField("gf_transfer_debit_date", DateType, true),
      StructField("gf_exchange_rate_agreed_per", DecimalType(12,9), true),
      StructField("gf_exchange_rate_agreed1_per", DecimalType(12,9), true),
      StructField("gf_benef_transfer_amount", DecimalType(26,6), true),
      StructField("g_credit_currency_id", StringType, true),
      StructField("gf_transfer_payment_date", DateType, true),
      StructField("g_frequency_transaction_type", StringType, true),
      StructField("gf_freq_trans_period_number", DecimalType(4,0), true),
      StructField("gf_period_creation_date", DateType, true),
      StructField("gf_period_end_date", DateType, true),
      StructField("gf_period_next_payment_date", DateType, true),
      StructField("g_period_valid_type", StringType, true),
      StructField("gf_payment_status_type", StringType, true),
      StructField("gf_transaction_reference_id", StringType, true),
      StructField("gf_movement_external_id", StringType, true),
      StructField("gf_nominal_amount", DecimalType(26,6), true),
      StructField("g_nominal_currency_id", StringType, true),
      StructField("gf_mt103_swift_id", StringType, true),
      StructField("gf_total_fees_amount", DecimalType(26,6), true),
      StructField("g_total_fees_currency_id", StringType, true),
      StructField("gf_transaction_comments_desc", StringType, true),
      StructField("gf_transfer_type", StringType, true),
      StructField("gf_value_date", DateType, true),
      StructField("gf_recurrent_transfer_ref_id", StringType, true),
      StructField("gf_fee_iban_id", StringType, true),
      StructField("gf_transfer_payment_amount", DecimalType(26,6), true),
      StructField("g_transfer_pay_currency_id", StringType, true),
      StructField("g_expenses_key_type", StringType, true),
      StructField("g_swift_message_ind_type", StringType, true),
      StructField("gf_basic_service_id", DecimalType(4,0), true),
      StructField("gf_transfer_priority_type", StringType, true),
      StructField("gf_channel_id", StringType, true),
      StructField("gf_channel_means_id", StringType, true),
      StructField("gf_means_service_id", StringType, true),
      StructField("gf_bizum_payer_alias_desc", StringType, true),
      StructField("gf_bizum_benef_alias_desc", StringType, true),
      StructField("gf_commerce_id", StringType, true),
      StructField("gf_commerce_name", StringType, true),
      StructField("gf_julian_date_number", DecimalType(7,0), true),
      StructField("gf_transfer_hms_date", TimestampType, true),
      StructField("gf_transfer_sequence_id", StringType, true),
      StructField("g_ordering_corresp_entity_id", StringType, true),
      StructField("gf_ord_corresp_entity_name", StringType, true),
      StructField("g_benef_corresp_entity_id", StringType, true),
      StructField("gf_benef_corresp_entity_name", StringType, true),
      StructField("gf_benef_corresp_account_id", StringType, true),
      StructField("gf_opern_audit_insert_date", TimestampType, true),
      StructField("gf_operational_audit_date", TimestampType, true),
      StructField("gf_audit_operuser_id", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true)
    ))
  }

  def getCustomSchemaForTransferDetail() : StructType = {
    StructType(Array(
      StructField("g_movement_id", StringType, true),
      StructField("gf_transfer_move_detail_id", StringType, true),
      StructField("gf_ref_transfer_move_detail_id", StringType, true),
      StructField("g_movement_type", StringType, true),
      StructField("gf_concept_id", StringType, true),
      StructField("g_banking_operation_id", StringType, true),
      StructField("gf_transfer_detail_amount", DecimalType(26,6), true),
      StructField("g_transfer_detail_ccy_id", StringType, true),
      StructField("gf_exchange_rate_agreed_per", DecimalType(12,9), true),
      StructField("gf_tax_rate_per", DecimalType(26,10), true),
      StructField("gf_tax_type", StringType, true),
      StructField("gf_operation_date", DateType, true),
      StructField("gf_opern_audit_insert_date", TimestampType, true),
      StructField("gf_operational_audit_date", TimestampType, true),
      StructField("gf_audit_operuser_id", StringType, true),
      StructField("gf_user_audit_id", StringType, true),
      StructField("gf_audit_date", TimestampType, true),
      StructField("gf_cutoff_date", DateType, true)
    ))
  }

  def createDate(date : String) : Date = {
    new Date((new SimpleDateFormat("dd/MM/yyyy").parse(date)).getTime)
  }

  def createTimestamp(timestamp : String) : Timestamp = {
    new Timestamp((new SimpleDateFormat("yyyy-MM-dd").parse(timestamp)).getTime)
  }

  def writeInParquetFile(data : Seq[Row], customSchema : StructType) : Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), customSchema)

    df.printSchema()
    df.show()
    df.coalesce(1).write.format("parquet").mode("append").save(pathToDestinationFiles)
    //df.coalesce(1).write.csv(pathToDestinationFiles)
  }

  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }

  def randomNumericString(length: Int): String = {
    val chars = ('0' to '9')
    randomStringFromCharList(length, chars)
  }

  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  def randomDecimal(length1: Int, length2: Int): java.math.BigDecimal = {
    val result = new StringBuilder

    val beforePoint = randomNumericString(length1)

    if(length2 == -1){
      result.append(beforePoint)
    }
    else if (length2 == 0) {
      result.append(beforePoint).append(".").append("0")
    }
    else {
      val afterPoint = randomNumericString(length2)
      result.append(beforePoint).append(".").append(afterPoint)
    }

    new java.math.BigDecimal(result.toString)
  }
}
