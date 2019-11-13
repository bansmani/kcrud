import com.google.gson.Gson
import org.h2.jdbcx.JdbcDataSource
import org.objenesis.ObjenesisStd
import java.sql.Connection
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.ArrayBlockingQueue

@Target(AnnotationTarget.CLASS)
annotation class Schema(val name: String)

@Target(AnnotationTarget.CLASS)
annotation class Entity(val name: String = "")

@Target(AnnotationTarget.FIELD)
annotation class Id

@Target(AnnotationTarget.FIELD)
annotation class Indexed


object DataSource : AutoCloseable {
    override fun close() {
        (1..5).forEach {
            pool.poll()?.close()
            println("closing $it")
        }
    }

    private val pool = ArrayBlockingQueue<Connection>(50)


    init {
        val ds = initDatasource()
        (1..5).forEach { _ ->
            pool.put(ds.connection)
        }
    }

    fun initDatasource(): JdbcDataSource {
        val prop =
            Properties().apply { load(ClassLoader.getSystemClassLoader().getResourceAsStream("kcrud.properties")) }
        val ds = Class.forName(prop.getProperty("datasourceclass")).newInstance() as JdbcDataSource
        ds.setURL(prop.getProperty("url"))
        ds.user = prop.getProperty("user")
        ds.password = prop.getProperty("password")
        return ds
    }

    fun getConnection(): Connection {
        return pool.take()
    }

    fun returnConnection(con: Connection) {
        pool.put(con)
    }
}

interface KCrudI {
    fun createTable(entity: Class<*>): Boolean
    //make it save or update single (for idempotent)
    //and add a new method insert which would throw exception
    fun save(entity: Any): Boolean

    fun update(entity: Any): Boolean

    fun update(entity: Class<*>, data: Map<String, Any>): Boolean
    fun update(entity: Class<*>, data: Any): Boolean

    fun execute(sql: String, silent: Boolean = false): Boolean

    fun find(sql: String): ResultSet

    fun <T> findList(type: Class<T>, sql: String): List<T>

    fun <T> find(entity: Class<T>, sql: String): List<T>
    fun <T> find(entity: Class<T>): List<T>

    fun <T> find(entity: Class<T>, map: Map<String, String>): List<T>
    //may not work if there are more than 1 column as id
    fun <T> findById(entity: Class<T>, value: Any): T?

    fun dropTable(entity: Class<*>): Boolean
    fun dropSchema(schema: String): Boolean
    fun truncateTable(entity: Class<*>): Boolean
    fun truncateAllTables(schemaName: String = ""): Boolean
    fun dropAllTables(): Boolean
}

private val gson = Gson()

object KCrud : KCrudI {

    override fun dropAllTables(): Boolean {
        val sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'"
        findList(String::class.java, sql).forEach {
            execute("DROP TABLE IF EXISTS $it")
        }
        return true
    }

    override fun truncateAllTables(schemaName: String): Boolean {
        val sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'"
        findList(String::class.java, sql).forEach {
            execute("TRUNCATE TABLE $it")
        }
        return true
    }


    override fun truncateTable(entity: Class<*>): Boolean {
        val tableName = getTableName(entity)
        val sql = "TRUNCATE TABLE $tableName"
        return execute(sql, true)
    }

    override fun dropTable(entity: Class<*>): Boolean {
        val tableName = getTableName(entity)
        val sql = "DROP TABLE IF EXISTS $tableName"
        return execute(sql)
    }

    override fun dropSchema(schema: String): Boolean {
        val sql = "DROP SCHEMA IF EXISTS $schema CASCADE"
        return execute(sql)
    }


    private const val createTableIfMissing: Boolean = true

    override fun update(entity: Any): Boolean {
        return update(entity::class.java, getColumnValueMap(entity))
    }

    override fun update(entity: Class<*>, data: Any): Boolean {
        //check if data is Map this method should not get called
        val str = gson.toJson(data)
        val map = gson.fromJson<Map<String, String>>(str, Map::class.java)
        return update(entity, map)
    }

    override fun update(entity: Class<*>, data: Map<String, Any>): Boolean {
        val tableName = getTableName(entity)
        var updateString = ""
        var conditionString = ""
        val idColumn = getIdColumn(entity)
        data.forEach { entry ->
            if (idColumn.contains(entry.key)) {
                conditionString += "${entry.key}='${entry.value}' and "
            } else {
                updateString += "${entry.key}='${entry.value}',"
            }
        }
        conditionString = conditionString.removeSuffix(" and ")
        updateString = updateString.removeSuffix(",")

        val sql = "UPDATE $tableName SET $updateString WHERE $conditionString"
        return execute(sql)
    }


    private fun getIdColumn(entity: Class<*>): Map<String, Class<*>> {
        val map = mutableMapOf<String, Class<*>>()
        entity.declaredFields.forEach {
            if (it.isAnnotationPresent(Id::class.java)) {
                map[it.name] = it.type
            }
        }
        return map
    }


    private val typeMap = mapOf(
        "String" to "VARCHAR",
        "Instant" to "TIMESTAMP",
        "LinkedList" to "VARCHAR",
        "List" to "VARCHAR",
        "Set" to "VARCHAR",
        "Map" to "VARCHAR"
    )

    override fun createTable(entity: Class<*>): Boolean {
        val tableName = getTableName(entity)
        val createTablePrefix = "CREATE TABLE IF NOT EXISTS $tableName ("
        var columnString = ""
        var pkColumns = ""
        val createIndexPrefix =
            "CREATE INDEX IF NOT EXISTS IDX_${tableName.replace(".", "_").toUpperCase()} ON $tableName("
        var indexedColumns = ""

        val fields = entity.declaredFields
        fields.forEach { field ->
            columnString += ", ${field.name} "

            columnString += typeMap[field.type.simpleName]
                ?: if (field.type.isEnum) "VARCHAR" else field.type.simpleName.toUpperCase()

            indexedColumns += if (field.isAnnotationPresent(Indexed::class.java)) field.name else ""

            pkColumns += if (field.isAnnotationPresent(Id::class.java)) field.name + "," else ""
        }

        columnString = columnString.removePrefix(",").trim()
        if (pkColumns != "") {
            pkColumns = pkColumns.removeSuffix(",")
            pkColumns = ", PRIMARY KEY ($pkColumns)"
        }
        columnString += pkColumns

        var sql = "$createTablePrefix$columnString); \n"
        if (indexedColumns != "") {
            sql += "$createIndexPrefix$indexedColumns); \n"
        }
        try {
            execute(sql)
        } catch (e: Exception) {
            val schemaName = tableName.split(".").first()
            if (e.message!!.toLowerCase().contains("schema \"$schemaName\" not found")) {
                execute("create schema if not exists $schemaName")
                execute(sql)
            }
        }
        return true
    }

    override fun execute(sql: String, silent: Boolean): Boolean {
        println(sql)
        val connection = DataSource.getConnection()
        return try {
            connection.prepareStatement(sql).execute()
        } catch (e: Exception) {
            if (!silent) {
                println(e.message)
                throw e
            }
            return false
        } finally {
            DataSource.returnConnection(connection)
        }
    }

    override fun save(entity: Any): Boolean {
        val tableName = getTableName(entity)
        val columns = getColumnsAsCSVString(entity)
        val values = getValuesAsCSVString(entity)

        val sql = "INSERT INTO $tableName ($columns) VALUES ($values)"
        return try {
            execute(sql)
        } catch (e: Exception) {
            if (createTableIfMissing) {
                createTable(entity.javaClass)
                execute(sql)
            } else false
        }
    }

    private fun getColumnsAsCSVString(entity: Any): String {
        return entity.javaClass.declaredFields.map { field -> field.name }
            .reduce { acc, field ->
                "$acc, $field"
            }.toString()
    }

    private fun getValuesAsCSVString(entity: Any): String {
        return entity.javaClass.declaredFields.map { field -> field.isAccessible = true; field.get(entity) }
            .map { any ->
                if (any == null || any == "") null else "'$any'"
            }
            .reduce { acc, value ->
                "$acc, $value"
            }.toString()
    }

    private fun getColumns(entity: Any): List<String> {
        return entity.javaClass.declaredFields.map { field -> field.name }
    }

    private fun getValues(entity: Any): List<String?> {
        return entity.javaClass.declaredFields
            .map { field -> field.isAccessible = true; field.get(entity) }
            .map { any -> if (any == null || any == "") null else "'$any'" }
    }

    private fun getColumnValueMap(entity: Any, skipNulls: Boolean = true): Map<String, Any> {
        return entity.javaClass.declaredFields
            .map { field -> field.isAccessible = true; field }
            .filter { field -> field.get(entity) != null }
            .map { field -> field.name to field.get(entity) }.toMap()
    }

    private fun getTableName(entity: Any): String {
        return getTableName(entity.javaClass)
    }

    private fun getTableName(entity: Class<*>): String {
        val tableName = if (entity.isAnnotationPresent(Entity::class.java))
            entity.getAnnotation(Entity::class.java).name + "."
        else
            entity.simpleName.split("$").last()

        val schemaName = if (entity.isAnnotationPresent(Schema::class.java))
            entity.getAnnotation(Schema::class.java).name + "."
        else
            ""
        return schemaName + tableName
    }

    override fun find(sql: String): ResultSet {
        println(sql)
        val connection = DataSource.getConnection()
        try {
            return connection.prepareStatement(sql).executeQuery()
        } finally {
            DataSource.returnConnection(connection)
        }

    }

    override fun <T> find(entity: Class<T>): List<T> {
        return find(entity, "select * from ${getTableName(entity)}")
    }

    override fun <T> findById(entity: Class<T>, value: Any): T? {
        val idColumn = getIdColumn(entity)
        val tableName = getTableName(entity)
        val sql = "SELECT * FROM $tableName WHERE  ${idColumn.keys.first()}='$value'"
        return find(entity, sql).getOrNull(0)
    }

    override fun <T> find(entity: Class<T>, map: Map<String, String>): List<T> {
        val tableName = getTableName(entity)
        var conditionString = ""
        map.forEach { entry ->
            conditionString += "${entry.key}='${entry.value}' and "
        }
        conditionString = conditionString.removeSuffix(" and ")

        val sql = "SELECT * FROM $tableName WHERE $conditionString"
        return find(entity, sql)
    }


    override fun <T> findList(type: Class<T>, sql: String): List<T> {
        val rs = try {
            find(sql)
        } catch (e: java.lang.Exception) {
            return emptyList()
        }
        val list = mutableListOf<Any>()
        while (rs.next()) {
            when (type.simpleName) {
                "long", "Long" -> list.add(rs.getLong(1))
                "double", "Double" -> list.add(rs.getDouble(1))
                "int", "Integer" -> list.add(rs.getInt(1))
                "String" -> list.add(rs.getString(1))
                "Instant" -> list.add(rs.getTimestamp(1).toInstant())
                "LocalDateTime" -> list.add(rs.getTimestamp(1).toLocalDateTime())
                "Date" -> list.add(rs.getDate(1))
                "LocalDate" -> list.add(rs.getDate(1).toLocalDate())
                "List" -> list.add(rs.getString(1).toList())
                "Set" -> list.add(rs.getString(1).toSet())
                "LinkedList" -> list.add(rs.getString(1).toLinkedList())
                // add for map
                else -> {
                    val enumclass = Class.forName(type.name)
                    val valueOf = enumclass.getDeclaredMethod("valueOf", String::class.java)
                    val enumvalue = valueOf.invoke(enumclass, rs.getString(1))
                    list.add(enumvalue)
                }
            }
        }
        return list as List<T>
    }

    override fun <T> find(entity: Class<T>, sql: String): List<T> {
        val rs = try {
            find(sql)
        } catch (e: java.lang.Exception) {
            return emptyList()
        }

        val colCaseInSensitiveMap = entity.declaredFields.map { it.name.toUpperCase() to it.name }.toMap()
        val list = arrayListOf<T>()
        while (rs.next()) {

            val instance = try {
                entity.newInstance()
            } catch (e: Exception) {
                ObjenesisStd().newInstance(entity)
            }
            for (i in 1..rs.metaData.columnCount) {
                val colname = rs.metaData.getColumnName(i)
                val field = entity.getDeclaredField(colCaseInSensitiveMap[colname])
                field.isAccessible = true
                when (field.type.simpleName) {
                    "long" -> field.setLong(instance, rs.getLong(i))
                    "double" -> field.setDouble(instance, rs.getDouble(i))
                    "int" -> field.setInt(instance, rs.getInt(i))
                    "String" -> field.set(instance, rs.getString(i))
                    "Instant" -> field.set(instance, rs.getTimestamp(i).toInstant())
                    "LocalDateTime" -> field.set(instance, rs.getTimestamp(i).toLocalDateTime())
                    "Date" -> field.set(instance, rs.getDate(i))
                    "LocalDate" -> field.set(instance, rs.getDate(i).toLocalDate())
                    "List" -> field.set(instance, rs.getString(i).toList())
                    "Set" -> field.set(instance, rs.getString(i).toSet())
                    "LinkedList" -> field.set(instance, rs.getString(i).toLinkedList())
                    // add for map
                    else -> {
                        val enumclass = Class.forName(field.type.name)
                        val valueOf = enumclass.getDeclaredMethod("valueOf", String::class.java)
                        val enumvalue = valueOf.invoke(enumclass, rs.getString(i))
                        field.set(instance, enumvalue)
                    }
                }
            }
            list.add(instance as T)
        }
        @Suppress("UNCHECKED_CAST") val array = list.toArray() as Array<T>
        return listOf(*array)
    }


    fun String.toLinkedList(): LinkedList<String> {
        val str = this
        return LinkedList<String>()
            .apply {
                addAll(
                    str.removePrefix("[")
                        .removeSuffix("]")
                        .split(", ")
                )
            }
    }

    fun String.toList(): List<String> {
        val str = this
        return ArrayList<String>()
            .apply {
                addAll(
                    str.removePrefix("[")
                        .removeSuffix("]")
                        .split(", ")
                )
            }
    }

    fun String.toSet(): Set<String> {
        val str = this
        return HashSet<String>()
            .apply {
                addAll(
                    str.removePrefix("[")
                        .removeSuffix("]")
                        .split(", ")
                )
            }
    }
}


