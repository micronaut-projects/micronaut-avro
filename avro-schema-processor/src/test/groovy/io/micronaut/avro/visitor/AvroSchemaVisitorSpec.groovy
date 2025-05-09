package io.micronaut.avro.visitor

import io.micronaut.avro.model.AvroSchema

class AvroSchemaVisitorSpec extends AbstractAvroSchemaSpec {

    void "test simple record schema generation"() {
        given:
        def avroSchema = buildAvroSchema('test.me.models.Salamander', 'Salamander', """
        package test.me.models;

        import io.micronaut.avro.Avro;
        import java.util.*;
        import java.math.BigDecimal;

        @Avro
        public record Salamander(
            String name,
            int age,
            Color color,
            List<List<String>> environments,
            BigDecimal bd,
            Map<String, Map<String, String>> doubleMap,
            byte zyt,
            Set<String> set,
            Salamander salamander,
            Vector<String> vector,
            short so
        ) {
            enum Color {
                RED,
                GREEN,
                BLUE
            }

}
""")
        expect:
        avroSchema.name == "Salamander"
        avroSchema.namespace == "test.me.models"
        avroSchema.fields.get(0).name == "age"
        avroSchema.fields.get(0).type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(1).name == "bd"
        avroSchema.fields.get(1).type.type == AvroSchema.Type.BYTES.name()
        avroSchema.fields.get(2).type.type == AvroSchema.Type.ENUM.name()
        avroSchema.fields.get(2).type.symbols == ["RED", "GREEN", "BLUE"]
        avroSchema.fields.get(2).name == "color"
    }

    void "test record schema with metadata"() {
        given:
        def avroSchema = buildAvroSchema('test.Salamander', 'salamander', """
        package test;

        import io.micronaut.avro.Avro;
        import java.time.LocalDate;
        import java.time.LocalTime;
        import java.util.*;

        @Avro(
                name = "salamander",
                doc = "this a salamander record"
        )
        public record Salamander(
                String name,
                int age,
                LocalDate date,
                LocalTime time,
                char character
        ) {
}
""")
        expect:
        avroSchema.name == "salamander"
        avroSchema.doc == "this a salamander record"
        avroSchema.namespace == "test"
        avroSchema.fields.get(0).name == "age"
        avroSchema.fields.get(0).type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(1).name == "character"
        avroSchema.fields.get(1).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(2).type.logicalType == AvroSchema.LogicalType.DATE.name()
        avroSchema.fields.get(2).name == "date"
        avroSchema.fields.get(2).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(2).type.logicalType == AvroSchema.LogicalType.DATE.name()
        avroSchema.fields.get(3).name == "name"
        avroSchema.fields.get(3).type == AvroSchema.Type.STRING.name()
        avroSchema.fields.get(4).name == "time"
        avroSchema.fields.get(4).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(4).type.logicalType == AvroSchema.LogicalType.TIME_MILLIS.name()
    }

    void "test record schema with nested lists"() {
        given:
        def avroSchema = buildAvroSchema('test.Salamander', 'salamander', """
        package test;

        import io.micronaut.avro.Avro;
        import java.time.LocalDate;
        import java.util.*;
        @Avro(
                name = "salamander",
                doc = "this a salamander class",
                aliases = {"test1, test2"}
        )
        public record Salamander(
            String name,
            boolean isTrue,
            Color color,
            List<List<String>> nestedList
        ) {
            enum Color {
            RED,
            GREEN,
            BLUE
        }

}
""")
        expect:
        avroSchema.name == "salamander"
        avroSchema.fields.get(0).name == "color"
        avroSchema.fields.get(0).type.symbols == ["RED", "GREEN", "BLUE"]
        avroSchema.fields.get(1).name == "isTrue"
        avroSchema.fields.get(1).type == AvroSchema.Type.BOOLEAN.name()
        avroSchema.fields.get(2).name == "name"
        avroSchema.fields.get(3).name == "nestedList"
        avroSchema.fields.get(3).type.type == AvroSchema.Type.ARRAY.name()
        avroSchema.fields.get(3).type.items.type == AvroSchema.Type.ARRAY.name()
        avroSchema.fields.get(3).type.items.items == AvroSchema.Type.STRING.name()
    }

    void "test simple class schema generation"() {
        given:
        def avroSchema = buildAvroSchema('test.Salamander', 'Salamander', """
        package test;

        import io.micronaut.avro.Avro;
        import java.time.LocalDate;
        import java.util.*;

        @Avro
        public class Salamander{
            String name;
            boolean isTrue;
            Salamander salamander;

            public String getName() {
                return name;
            }
            public Salamander getSalamander(){
                return salamander;
            }

        }
""")
        expect:
        avroSchema.name == "Salamander"
    }

    void "test schema with nested class"() {
        given:
        def avroSchema = buildAvroSchema('test.Salamander', 'Salamander', """
        package test;

        import io.micronaut.avro.Avro;

        import java.time.Duration;import java.time.LocalDate;import java.util.*;

        @Avro(
                name = "Salamander",
                doc = "this a salamander record"
        )
        public record Salamander(
                Male z,
                LocalDate localDate,
                UUID uuid,
                Duration duration,
                Date date

        ) {
            class Male {
                int age;
                String name;


                public int getAge() {
                    return age;
                }
                public String getName() {
                    return name;
                }
            }


}
""")
    }

    void "test schema with multiple types"(){
        given:
        def avroSchema = buildAvroSchema('test.Salamander', 'Salamander', """
        package dev.me.models;

        import io.micronaut.avro.Avro;
        import java.math.BigDecimal;
        import java.math.BigInteger;
        import java.sql.Timestamp;
        import java.time.Duration;
        import java.time.LocalDate;
        import java.time.OffsetDateTime;
        import java.time.OffsetTime;
        import java.util.*;

        @Avro
        public record Salamander (
                String name,
                int age,
                float a,
                Color color,
                List<List<String>> environments,
                BigDecimal bd,
                UUID uuid,
                Map<String, Map<String, String>>  doubleMap,
                boolean isOkay,
                BigInteger bigInt,
                Date date,
                Timestamp timestamp,
                Salamander salamander,
                OffsetTime offsetTime,
                Duration duration,
                Character character,
                char aChar,
                Set<String> stringSet,
                byte zyt,
                short aShort,
                Male male,
                Arrays arrays,
                Vector<String> vector
        ){
            class Male {
                int age;
                String name;


                public int getAge() {
                    return age;
                }
                public String getName() {
                    return name;
                }
            }
            enum Color {
                RED,
                GREEN,
                BLUE
            }
        }
""")
    }
    void "schema with primitive types" () {
        given:
        def  avroSchema = buildAvroSchema("test.Person", "Person", """

        package dev.test.avro;

        import io.micronaut.avro.Avro;

        @Avro
        public record Person (
                int testInt,
                float testFloat,
                double testDouble,
                long testLong,
                byte testByte,
                short testShort,
                char testChar,
                Character character
        ){}
""")
        expect:
        avroSchema.name == "Person"
        avroSchema.fields.get(0).name == "character"
        avroSchema.fields.get(0).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(0).type.javaClass == "java.lang.Character"
        avroSchema.fields.get(1).name == "testByte"
        avroSchema.fields.get(1).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(1).type.javaClass == "java.lang.Byte"
        avroSchema.fields.get(2).name == "testChar"
        avroSchema.fields.get(2).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(2).type.javaClass == "java.lang.Character"
        avroSchema.fields.get(3).name == "testDouble"
        avroSchema.fields.get(3).type == AvroSchema.Type.DOUBLE.name()
        avroSchema.fields.get(4).name == "testFloat"
        avroSchema.fields.get(4).type == AvroSchema.Type.FLOAT.name()
        avroSchema.fields.get(5).name == "testInt"
        avroSchema.fields.get(5).type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(6).name == "testLong"
        avroSchema.fields.get(6).type == AvroSchema.Type.LONG.name()
        avroSchema.fields.get(7).name == "testShort"
        avroSchema.fields.get(7).type.type == AvroSchema.Type.INT.name()
        avroSchema.fields.get(7).type.javaClass == "java.lang.Short"
    }

    void "schema with list of schema type" (){
        given :
        def avroSchema = buildAvroSchema("test.Person", "Person", """

        package dev.test.avro;

        import io.micronaut.avro.Avro;
        import java.util.List;
        @Avro
        public record Person(
                Person person,
                List<Person> personList
        ){

        }
""")
        expect:
        avroSchema.name == "Person"
        avroSchema.type == AvroSchema.Type.RECORD.name()
        avroSchema.fields.get(0).name == "person"
        avroSchema.fields.get(0).type == "Person"
        avroSchema.fields.get(1).name == "personList"
        avroSchema.fields.get(1).type.type == AvroSchema.Type.ARRAY.name()
        avroSchema.fields.get(1).type.javaClass == "java.util.List"
    }

}
