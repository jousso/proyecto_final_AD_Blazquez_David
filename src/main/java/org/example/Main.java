package org.example;

import javax.swing.plaf.nimbus.State;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.DecimalFormat;

public class Main {
    private static String url = "jdbc:sqlite:company_database.db";
    public static void main(String[] args) throws FileNotFoundException {
       queryMostrarDatos();
       crearTablas();
       insertarDatos();
    }

    public static void queryMostrarDatos() {
        String query = """
            SELECT
                p.project_id AS project_id,
                sc.coste_Salario_total AS pSalary,
                p.project_id AS id_project1,
                p.budget  AS budget,
                (sc.coste_Salario_total / p.budget) AS projectFraccion
            FROM projects p
            INNER JOIN (
                SELECT
                    ep.project_id,
                    SUM((er.salary / 1900) * ep.hours_worked) AS coste_Salario_total
                FROM employee_projects ep
                INNER JOIN employees_realistic er ON ep.employee_id = er.employee_id
                GROUP BY ep.project_id
            ) AS sc ON p.project_id = sc.project_id
        """;

        try (Connection connection = DriverManager.getConnection(url);
             PreparedStatement preparedStatement = connection.prepareStatement(query);
             ResultSet resultados = preparedStatement.executeQuery()) {

            System.out.printf("%-10s | %-15s | %-10s | %-13s |%-15s%n", "project_id", "Salario_projecto", "id_project1", "presupuesto", "project_Fraccion");
            System.out.println("------------------------------------------------------------------------------");

            while (resultados.next()) {
                int project_id = resultados.getInt("project_id");
                double pSalary = resultados.getDouble("pSalary");
                int id_project1 = resultados.getInt("id_project1");
                double budget = resultados.getDouble("budget");
                double projectFraccion = resultados.getDouble("projectFraccion");


                System.out.printf("%-10d | %-15.1f | %-10d | %-13.1f | %-15.1f%n", project_id, pSalary, id_project1, budget, projectFraccion * 100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void crearTablas() {
        String[] createTables = {
                "CREATE TABLE IF NOT EXISTS customers (" +
                        "customer_id INT PRIMARY KEY," +
                        "customer_name VARCHAR(255)," +
                        "contact_email VARCHAR(255)," +
                        "contact_phone VARCHAR(20))",

                "CREATE TABLE IF NOT EXISTS departments (" +
                        "department_id INT PRIMARY KEY," +
                        "department_name VARCHAR(255)," +
                        "manager_id INT)",

                "CREATE TABLE IF NOT EXISTS employee_projects (" +
                        "employee_id INT," +
                        "project_id INT," +
                        "hours_worked DECIMAL(10, 2)," +
                        "PRIMARY KEY (employee_id, project_id))",

                "CREATE TABLE IF NOT EXISTS employees_realistic (" +
                        "employee_id INT PRIMARY KEY," +
                        "first_name VARCHAR(255)," +
                        "last_name VARCHAR(255)," +
                        "department_id INT," +
                        "hire_date DATE," +
                        "salary DECIMAL(15, 2)," +
                        "position VARCHAR(100)," +
                        "FOREIGN KEY (department_id) REFERENCES departments(department_id))",

                "CREATE TABLE IF NOT EXISTS order_items (" +
                        "order_item_id INT PRIMARY KEY," +
                        "order_id INT," +
                        "product_name VARCHAR(255)," +
                        "quantity INT," +
                        "price DECIMAL(10, 2)," +
                        "FOREIGN KEY (order_id) REFERENCES orders(order_id))",

                "CREATE TABLE IF NOT EXISTS orders (" +
                        "order_id INT PRIMARY KEY," +
                        "customer_id INT," +
                        "order_date DATE," +
                        "amount DECIMAL(15, 2)," +
                        "FOREIGN KEY (customer_id) REFERENCES customers(customer_id))",

                "CREATE TABLE IF NOT EXISTS projects (" +
                        "project_id INT PRIMARY KEY," +
                        "project_name VARCHAR(255)," +
                        "department_id INT," +
                        "budget DECIMAL(15, 2)," +
                        "start_date DATE," +
                        "end_date DATE," +
                        "FOREIGN KEY (department_id) REFERENCES departments(department_id))"
        };
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {

            for (String sql : createTables) {
                statement.execute(sql);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void insertarDatos() throws FileNotFoundException {
        String archivoEmployeesProjects = "src/main/resources/customers.csv";
        String archivoDepartments = "src/main/resources/departments.csv";
        String archivoEmployeesRealistic = "src/main/resources/employees_realistic.csv";
        String archivoOrderItems = "src/main/resources/order_items.csv";
        String archivoOrders = "src/main/resources/orders.csv";
        String archivoProjects = "src/main/resources/projects.csv";


        String insertarSQLCustomers = "INSERT OR REPLACE INTO customers (customer_id, customer_name, contact_email, contact_phone) VALUES (?, ?, ?, ?)";
        String insertarSQLDepartments = "INSERT OR REPLACE INTO departments (department_id, department_name, manager_id) VALUES (?, ?, ?)";
        String insertarSQLEmployeesRealistic = "INSERT OR REPLACE INTO employees_realistic (employee_id, first_name, last_name, department_id, hire_date, salary, position) VALUES (?, ?, ?, ?, ?, ?, ?)";
        String insertarSQLOrderItems = "INSERT OR REPLACE INTO order_items (order_item_id, order_id, product_name, quantity, price) VALUES (?, ?, ?, ?, ?)";
        String insertarSQLOrders = "INSERT OR REPLACE INTO orders (order_id, customer_id, order_date, amount) VALUES (?, ?, ?, ?)";
        String insertarSQLProjects = "INSERT OR REPLACE INTO projects (project_id, project_name, department_id, budget, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?)";


        try (BufferedReader br = new BufferedReader(new FileReader(archivoEmployeesProjects)) ;
        PreparedStatement pStmnt = DriverManager.getConnection(url).prepareStatement(insertarSQLCustomers)) {
            String line;
            boolean esPrimeraLinea = true;
            while ((line = br.readLine()) != null) {
                if (esPrimeraLinea) {
                    esPrimeraLinea = false;
                    continue;
                }
                String[] values = line.split(",");
                pStmnt.setInt(1, Integer.parseInt(values[0]));
                pStmnt.setString(2, values[1]);
                pStmnt.setString(3, values[2]);
                pStmnt.setString(4, values[3]);

                pStmnt.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(archivoDepartments)) ;
             PreparedStatement pStmnt = DriverManager.getConnection(url).prepareStatement(insertarSQLDepartments)) {
            String line;
            boolean esPrimeraLinea = true;
            while ((line = br.readLine()) != null) {
                if (esPrimeraLinea) {
                    esPrimeraLinea = false;
                    continue;
                }
                String[] values = line.split(",");
                pStmnt.setInt(1, Integer.parseInt(values[0]));
                pStmnt.setString(2, values[1]);
                pStmnt.setInt(3, Integer.parseInt(values[2]));

                pStmnt.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(archivoEmployeesRealistic)) ;
             PreparedStatement pStmnt = DriverManager.getConnection(url).prepareStatement(insertarSQLEmployeesRealistic)) {
            String line;
            boolean esPrimeraLinea = true;
            while ((line = br.readLine()) != null) {
                if (esPrimeraLinea) {
                    esPrimeraLinea = false;
                    continue;
                }
                String[] values = line.split(",");
                pStmnt.setInt(1, Integer.parseInt(values[0]));
                pStmnt.setString(2, values[1]);
                pStmnt.setString(3, values[2]);
                pStmnt.setInt(4, Integer.parseInt(values[3]));
                pStmnt.setDate(5, Date.valueOf(values[4]));
                pStmnt.setDouble(6, Double.parseDouble(values[5]));
                pStmnt.setString(7, values[6]);

                pStmnt.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(archivoOrderItems)) ;
             PreparedStatement pStmnt = DriverManager.getConnection(url).prepareStatement(insertarSQLOrderItems)) {
            String line;
            boolean esPrimeraLinea = true;
            while ((line = br.readLine()) != null) {
                if (esPrimeraLinea) {
                    esPrimeraLinea = false;
                    continue;
                }
                String[] values = line.split(",");
                pStmnt.setInt(1, Integer.parseInt(values[0]));
                pStmnt.setInt(2, Integer.parseInt(values[1]));
                pStmnt.setString(3, values[2]);
                pStmnt.setInt(4, Integer.parseInt(values[3]));
                pStmnt.setDouble(5, Double.parseDouble(values[4]));

                pStmnt.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(archivoOrders)) ;
             PreparedStatement pStmnt = DriverManager.getConnection(url).prepareStatement(insertarSQLOrders)) {
            String line;
            boolean esPrimeraLinea = true;
            while ((line = br.readLine()) != null) {
                if (esPrimeraLinea) {
                    esPrimeraLinea = false;
                    continue;
                }
                String[] values = line.split(",");
                pStmnt.setInt(1, Integer.parseInt(values[0]));
                pStmnt.setInt(2, Integer.parseInt(values[1]));
                pStmnt.setDate(3, Date.valueOf(values[2]));
                pStmnt.setDouble(4, Double.parseDouble(values[3]));

                pStmnt.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(archivoProjects)) ;
             PreparedStatement pStmnt = DriverManager.getConnection(url).prepareStatement(insertarSQLProjects)) {
            String line;
            boolean esPrimeraLinea = true;
            while ((line = br.readLine()) != null) {
                if (esPrimeraLinea) {
                    esPrimeraLinea = false;
                    continue;
                }
                String[] values = line.split(",");
                pStmnt.setInt(1, Integer.parseInt(values[0]));
                pStmnt.setString(2, values[1]);
                pStmnt.setInt(3, Integer.parseInt(values[2]));
                pStmnt.setDouble(4, Double.parseDouble(values[3]));
                pStmnt.setDate(5, Date.valueOf(values[4]));
                pStmnt.setDate(6, Date.valueOf(values[5]));

                pStmnt.executeUpdate();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }





    }

}