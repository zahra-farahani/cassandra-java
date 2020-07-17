package repository;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.Drop;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import domain.Employee;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static util.ConstantUtils.Employee.*;
import static  com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class EmployeeRepository {

    private static final String TABLE_NAME = "employee";
    private Session session;

    public EmployeeRepository(Session session) {
        this.session = session;
    }

    public void save(Employee employee){
        Statement saveEmployeeStatement = QueryBuilder.insertInto(TABLE_NAME)
                .value(EMPLOYEE_ID,employee.getEmployeeId())
                .value(FIRST_NAME,employee.getFirstName())
                .value(LAST_NAME,employee.getLastName())
                .value(EMPLOYEE_CODE,employee.getEmployeeCode());

        session.execute(saveEmployeeStatement);
    }

    public void update(Employee employee){
        Statement updateEmployeeStatement = QueryBuilder.update(TABLE_NAME)
                .with(set(FIRST_NAME, employee.getFirstName()))
                .and(set(LAST_NAME, employee.getLastName()))
                .and(set(EMPLOYEE_CODE, employee.getEmployeeCode()))
                .where(eq(EMPLOYEE_ID, employee.getEmployeeId()));

        session.execute(updateEmployeeStatement);
    }

    public void delete(UUID employeeId){
        Delete.Where delete = QueryBuilder.delete()
                .from(TABLE_NAME)
                .where(eq(EMPLOYEE_ID, employeeId));

        session.execute(delete);
    }

    public void deleteAll(){
        Truncate delete = QueryBuilder.truncate(TABLE_NAME);

        session.execute(delete);
    }

    public Employee findOne(UUID employeeId){
        Select.Where selectEmployeeById = QueryBuilder.select()
                .from(TABLE_NAME)
                .where(eq(EMPLOYEE_ID, employeeId));
        Row row = session.execute(selectEmployeeById).one();

        if (row != null) {
            Employee employee = new Employee(row.getUUID(EMPLOYEE_ID), row.getString(FIRST_NAME),
                    row.getString(LAST_NAME), row.getString(EMPLOYEE_CODE));
            return employee;
        }

        return null;
    }

    public List<Employee> findAll(){
        Select selectAllEmployees = select().all().from(TABLE_NAME);
        ResultSet employees = session.execute(selectAllEmployees);

        List<Employee> employeeArrayList = new ArrayList<>();
        employees.forEach(e -> employeeArrayList.add(new Employee(e.getUUID(EMPLOYEE_ID), e.getString(FIRST_NAME),
                e.getString(LAST_NAME), e.getString(EMPLOYEE_CODE))));

        return employeeArrayList;
    }

    public void createTable(String keyspace){
        Create createEmployeeTable = SchemaBuilder.createTable(keyspace, TABLE_NAME).ifNotExists()
                .addPartitionKey(EMPLOYEE_ID, DataType.uuid())
                .addColumn(EMPLOYEE_CODE, DataType.text())
                .addColumn(FIRST_NAME, DataType.text())
                .addColumn(LAST_NAME, DataType.text());

        session.execute(createEmployeeTable);
    }

    public void deleteTable(String keyspace){
        Drop dropEmployeeTable = SchemaBuilder.dropTable(keyspace, TABLE_NAME).ifExists();

        session.execute(dropEmployeeTable);
    }

}
