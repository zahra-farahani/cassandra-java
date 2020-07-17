package repository;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import connector.CassandraConnector;
import domain.Employee;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class EmployeeRepositoryTest {

    private static KeyspaceRepository keyspaceRepository;
    private static EmployeeRepository employeeRepository;
    private static Session session;

    private final static String KEYSPACE_NAME = "Test";
    private final static String TABLE_NAME = "employee";

    @BeforeClass
    public static void connect(){
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042);
        session = connector.getSession();

        keyspaceRepository = new KeyspaceRepository(session);
        keyspaceRepository.createKeySpace(KEYSPACE_NAME, "SimpleStrategy", 1);
        keyspaceRepository.useKeyspace(KEYSPACE_NAME);

        employeeRepository = new EmployeeRepository(session);
        employeeRepository.deleteTable(KEYSPACE_NAME);
        employeeRepository.createTable(KEYSPACE_NAME);
    }

    @Before
    public void init() {
        employeeRepository.deleteAll();
    }

    @Test
    public void whenCreatingATable_thenCreatedCorrectly() {
        employeeRepository.createTable(KEYSPACE_NAME);

        Select selectQuery = QueryBuilder.select().all().from(KEYSPACE_NAME, TABLE_NAME);
        int columnsSize = session.execute(selectQuery).getColumnDefinitions().size();

        assertEquals(columnsSize, 4);
    }

    @Test
    public void whenAddingANewEmployee_thenEmployeeExists() {
        Employee newEmployee = new Employee(UUID.randomUUID(), "Zahra", "Shahrabi", "245");
        employeeRepository.save(newEmployee);

        Employee savedEmployee = employeeRepository.findOne(newEmployee.getEmployeeId());

        assertNotEquals(savedEmployee,null);
        assertEquals(savedEmployee.getEmployeeId(),newEmployee.getEmployeeId());
    }

    @Test
    public void whenSelectingAll_thenReturnAllRecords() {
        Employee newEmployee = new Employee(UUID.randomUUID(), "Zahra", "Shahrabi", "245");
        employeeRepository.save(newEmployee);

        newEmployee = new Employee(UUID.randomUUID(), "Sahar", "Salehi", "290");
        employeeRepository.save(newEmployee);

        List<Employee> employees = employeeRepository.findAll();

        assertEquals(2, employees.size());
    }

    @Test
    public void whenSelectingOneById_thenReturnOne() {
        Employee newEmployee = new Employee(UUID.randomUUID(), "Zahra", "Shahrabi", "245");
        employeeRepository.save(newEmployee);

        Employee savedEmployee = employeeRepository.findOne(newEmployee.getEmployeeId());

        assertEquals(savedEmployee.getEmployeeId(), newEmployee.getEmployeeId());
    }

    @Test
    public void whenDeletingAnEmployeeById_thenEmployeeIsDeleted() {
        Employee newEmployee = new Employee(UUID.randomUUID(), "Zahra", "Shahrabi", "245");
        employeeRepository.save(newEmployee);

        employeeRepository.delete(newEmployee.getEmployeeId());

        Employee fetchedEmployee = employeeRepository.findOne(newEmployee.getEmployeeId());

        assertEquals(fetchedEmployee,null);
    }

    @Test
    public void whenUpdatingAnEmployee_thenEmployeeIsUpdated() {
        Employee newEmployee = new Employee(UUID.randomUUID(), "Zahra", "Shahrabi", "245");
        employeeRepository.save(newEmployee);

        newEmployee.setFirstName("Sahar");
        newEmployee.setLastName("Salehi");
        newEmployee.setEmployeeCode("290");
        employeeRepository.update(newEmployee);

        Employee updatedEmployee = employeeRepository.findOne(newEmployee.getEmployeeId());

        assertEquals(updatedEmployee.getFirstName(),newEmployee.getFirstName());
        assertEquals(updatedEmployee.getLastName(),newEmployee.getLastName());
        assertEquals(updatedEmployee.getEmployeeCode(), newEmployee.getEmployeeCode());
    }
}