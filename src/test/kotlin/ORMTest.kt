import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

class ORMTest {

    @BeforeEach
    fun before() {
        KCrud.execute("drop schema if exists test CASCADE")
        KCrud.execute("create schema if not exists test")
    }

    @Test
    fun `test execute with query resultset impl`() {
        //create table
        val createTableSQL = "create table test.DummyModel (data VARCHAR, anint INT)"
        KCrud.execute(createTableSQL)
        val tableName = KCrud.find("show tables from test").apply { first() }.getString("TABLE_NAME")
        assertThat(tableName).isEqualToIgnoringCase("DummyModel")
    }

    @Test
    fun `test ORM Save and Map model`() {
        @Schema("test")
        data class DummyModel2(val data: String, val anint: Int)

        KCrud.save(DummyModel2("a dummy data", 100))

        val query = KCrud.find(DummyModel2::class.java, "Select * from test.DummyModel2").first()

        assertThat(query.data == "a dummy data")
        assertThat(query.anint == 100)
    }


    @Test
    fun `create table test`() {
        //define model
        @Schema("test")
        data class DummyModel3(@Id val name: String, @Indexed val data: String, val anint: Int)
        KCrud.createTable(DummyModel3::class.java)
    }

    @Test
    fun `create table with composit pk`() {
        //define model
        @Schema("test")
        data class DummyModel4(@Id val name: String, @Indexed val data: String, @Id val anint: Int, val dob: Instant)
        KCrud.createTable(DummyModel4::class.java)
    }



}
