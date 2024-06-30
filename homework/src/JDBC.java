import example.Department;
import example.Person;
import h2.jdbc.JdbcConnectionBackwardsCompat;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class JDBC {
    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
            createTableDepartment(connection);
            insertDataDepartment(connection);
            createTable(connection);
            insertData(connection);
            selectAllPerson(connection);

            int personId = 1;
            System.out.println("Person # " + personId + " is in department " + nameDepartmentByIdPerson(connection, personId));
            System.out.println("Person name and his department");
            personAndDepartment(connection);
            System.out.println("Department and emploeers");
            departmentNumberAndAllPerson(connection);
            objectPersonAndDepartments(connection);
            objectDepartmentAndListPersons(connection);



        } catch (SQLException e) {
            System.err.println("Во время подключения произошла ошибка: " + e.getMessage());
        }
    }

    private static void selectAllPerson(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("Select id,departmentId, name, age from person");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                long idDepartment = resultSet.getLong("departmentId");
                String name = resultSet.getString("name");
                int age = resultSet.getByte("age");
                System.out.println("Найдена строка: [id = " + id + ", department ID = " + idDepartment + ", name = " + name + ", age = " + age + "]");
            }

        }
    }

    // 2. Создать таблицу Department (id bigint primary key, name varchar(128) not null)
    private static void createTableDepartment(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table department (
                    id bigint primary key,
                    name varchar(128) not null
                    )
                    """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка : " + e.getMessage());
            throw e;
        }

    }

    //    2. Создать таблицу Department (id bigint primary key, name varchar(128) not nulll
    private static void insertDataDepartment(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insert = new StringBuilder("insert into department (id,name) values\n");
            for (int i = 1; i <= 10; i++) {

                insert.append(String.format("(%s, '%s')", i, "department # " + i));

                if (i != 10) insert.append(",\n");

            }
            int insertCount = statement.executeUpdate(insert.toString());
            System.out.println("Вставлено строк: " + insertCount);
        }
    }

    // 3. Добавить в таблицу Person поле department_id типа bigint (внешний ключ)
    private static void createTable(Connection connection) throws SQLException {

        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table person (
                    id bigint primary key,
                    departmentId bigint,
                    name varchar(256),
                    age Integer,
                    active boolean,
                    foreign key (departmentID) references department(id)
                    )
                    """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка : " + e.getMessage());
            throw e;
        }

    }

    private static void insertData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insert = new StringBuilder("insert into person(id, departmentId, name, age, active) values\n");
            for (int i = 1; i <= 10; i++) {
                int age = ThreadLocalRandom.current().nextInt(20, 60);
                boolean active = ThreadLocalRandom.current().nextBoolean();
                int departmentId = ThreadLocalRandom.current().nextInt(1, 10);
                insert.append(String.format("(%s,%s, '%s',%s,%s)", i, departmentId, "Person # " + i, age, active));

                if (i != 10) insert.append(",\n");

            }
            int insertCount = statement.executeUpdate(insert.toString());
            System.out.println("Insert strings: " + insertCount);
        }
    }

    private static void selectData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("Select id, name, age from person where active is true");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getByte("age");
                System.out.println("Found string: [id = " + id + ", name = " + name + ", age = " + age + "]");
            }

        }
    }

    private static void updateDate(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int updateCount = statement.executeUpdate("update person set active = true where id > 5");
            System.out.println("Обновлено строк : " + updateCount);
        }
    }

    private static List<String> selectNameByAge(Connection connection, String age) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select name from person where age = ?")) {
            statement.setInt(1, Integer.parseInt(age));
            ResultSet resultSet = statement.executeQuery();
            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                names.add(resultSet.getString("name"));
            }

            return names;
        }
    }

    // 4. Написать метод, который загружает Имя department по Идентификатору person
    private static String nameDepartmentByIdPerson(Connection connection, int id) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("Select departmentId from person where id =  " + id);
            if (!resultSet.next()) throw new NoSuchElementException("Person # " + id + " not found");
            String dp = resultSet.getString("departmentId");
            ResultSet resultSet1 = statement.executeQuery("select name from department where id =" + dp);
            if (!resultSet1.next()) throw new NoSuchElementException("Department # " + dp + " не найден");
            return resultSet1.getString("name");

        }
    }

    // 5. * Написать метод, который загружает Map<String, String>, в которой маппинг person.name -> department.name
    private static Map<String, String> personAndDepartment(Connection connection) throws SQLException {
        Map<String, String> personAndDepartment = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("Select name, departmentId from person");
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String dp = resultSet.getString("departmentId");
                try (Statement innerStatment = connection.createStatement()) {
                    ResultSet resultSet1 = innerStatment.executeQuery("select name from department where id = " + dp);
                    if (!resultSet1.next()) throw new NoSuchElementException("This is not department for this emploee");
                    String nameDp = resultSet1.getString("name");
                    personAndDepartment.put(name, nameDp);
                }

            }
            for (Map.Entry<String, String> entry : personAndDepartment.entrySet()) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }
        }

        return personAndDepartment;
    }

    // 6. ** Написать метод, который загружает Map<String, List<String>>, в которой маппинг department.name -> <person.name>
    private static Map<String, List<String>> departmentNumberAndAllPerson(Connection connection) throws SQLException {
        Map<String, List<String>> departmentNumberAndAllPerson = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT d.name AS department_name, p.name AS person_name\n" +
                    "FROM department d\n" +
                    "JOIN person p ON d.id = p.departmentId;\n");
            while (resultSet.next()) {
                String dpId = resultSet.getString("department_name");
                String personName = resultSet.getString("person_name");

                if (departmentNumberAndAllPerson.containsKey(dpId)) {
                    departmentNumberAndAllPerson.get(dpId).add(personName);
                } else {
                    List<String> personNames = new ArrayList<>();
                    personNames.add(personName);
                    departmentNumberAndAllPerson.put(dpId, personNames);
                }
            }

        }

        for (Map.Entry<String, List<String>> entry : departmentNumberAndAllPerson.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());

        }

        return departmentNumberAndAllPerson;
    }

    // 7. *** Создать классы-обертки над таблицами, и в пунктах 4, 5, 6 возвращать объекты.
    private static Map<Person, Department> objectPersonAndDepartments(Connection connection) throws SQLException {
        Map<Person, Department> objectPersonAndDepartments = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT p.id AS person_id," +
                    " p.name AS person_name, p.age AS person_age, p.active AS person_active,\n" +
                    "       d.id AS department_id, d.name AS department_name\n" +
                    "FROM person p\n" +
                    "JOIN department d ON p.departmentId = d.id;\n");
            while (resultSet.next()) {
                long id = resultSet.getLong("person_id");
                String departmentId = resultSet.getString("department_name");
                String name = resultSet.getString("person_name");
                int age = resultSet.getInt("person_age");
                boolean active = resultSet.getBoolean("person_active");
                long dptId = resultSet.getLong("department_id");
                String dptName = resultSet.getString("department_name");
                Department department = new Department(dptId, dptName);
                Person person = new Person(id, department, name, age, active);
                objectPersonAndDepartments.put(person, department);

            }
        }
        for (Map.Entry<Person, Department> entry : objectPersonAndDepartments.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());

        }
        return objectPersonAndDepartments;
    }

    // 7. *** Создать классы-обертки над таблицами, и в пунктах 4, 5, 6 возвращать объекты.
    private static Map<Department, List<Person>> objectDepartmentAndListPersons(Connection connection) throws SQLException {
        Map<Department, List<Person>> objectDepartmentAndListPersons = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT d.id AS department_id, d.name AS department_name,\n" +
                    "       p.id AS person_id, p.name AS person_name, p.age AS person_age, p.active AS person_active\n" +
                    "FROM department d\n" +
                    "LEFT JOIN person p ON d.id = p.departmentId;\n");
            while (resultSet.next()) {
                long id = resultSet.getLong("person_id");
                String departmentId = resultSet.getString("department_name");
                String name = resultSet.getString("person_name");
                int age = resultSet.getInt("person_age");
                boolean active = resultSet.getBoolean("person_active");
                long dptId = resultSet.getLong("department_id");
                String dptName = resultSet.getString("department_name");
                Department department = new Department(dptId, dptName);
                Person person = new Person(id, department, name, age, active);


                if (objectDepartmentAndListPersons.containsKey(department)) {
                    objectDepartmentAndListPersons.get(department).add(person);
                } else {

                    List<Person> persons = new ArrayList<>();
                    persons.add(person);
                    objectDepartmentAndListPersons.put(department, persons);
                }
            }
        }
        for (Map.Entry<Department, List<Person>> entry : objectDepartmentAndListPersons.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }
        return objectDepartmentAndListPersons;
    }

}
