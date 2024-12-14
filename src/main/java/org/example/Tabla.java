package org.example;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.DecimalFormat;

public class Tabla {

    private JPanel JpanelP;
    private JTable table1;
    private String url = "jdbc:sqlite:company_database.db";



    public Tabla() throws FileNotFoundException {
        table1.setModel(new DefaultTableModel(
                new Object[][]{},
                new String[]{"Project ID", "Salary Cost", "Project ID 1", "Budget", "Project Fraction"}
        ));
        table1.setRowHeight(25);
        table1.getTableHeader().setFont(new Font("SansSerif", Font.BOLD, 12));
        table1.getTableHeader().setBackground(new Color(200, 200, 200));
        table1.getTableHeader().setReorderingAllowed(false);
        table1.getColumnModel().getColumn(0).setPreferredWidth(100);
        table1.getColumnModel().getColumn(1).setPreferredWidth(150);
        table1.getColumnModel().getColumn(2).setPreferredWidth(100);
        table1.getColumnModel().getColumn(3).setPreferredWidth(100);
        table1.getColumnModel().getColumn(4).setPreferredWidth(150);
        table1.setFont(new Font("SansSerif", Font.PLAIN, 12));
        table1.setBackground(new Color(245, 245, 245));
        table1.setGridColor(new Color(200, 200, 200));
        JScrollPane scrollPane = new JScrollPane(table1);
        JpanelP = new JPanel(new BorderLayout());
        JpanelP.add(scrollPane, BorderLayout.CENTER);
        crearTablas();
        insertarDatos();
        queryMostrarDatos();
    }

    public void queryMostrarDatos() {
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

            DefaultTableModel model = (DefaultTableModel) table1.getModel();
            model.setRowCount(0);
            DecimalFormat df = new DecimalFormat("#.0");

            while (resultados.next()) {
                int project_id = resultados.getInt("project_id");
                double pSalary = resultados.getDouble("pSalary");
                int id_project1 = resultados.getInt("id_project1");
                double budget = resultados.getDouble("budget");
                double projectFraccion = resultados.getDouble("projectFraccion");

                model.addRow(new Object[]{ project_id, df.format(pSalary), id_project1, df.format(budget), df.format(projectFraccion * 100)
                });            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void crearTablas() {
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



    public void insertarDatos() throws FileNotFoundException {
        String archivoEmployeesProjects = "src/main/resources/customers.csv";
        String archivoDepartments = "src/main/resources/departments.csv";
        String archivoEmployeesRealistic = "src/main/resources/employees_realistic.csv";
        String archivoOrderItems = "src/main/resources/order_items.csv";
        String archivoOrders = "src/main/resources/orders.csv";
        String archivoProjects = "src/main/resources/projects.csv";


        String insertarSQLCustomers = "INSERT OR IGNORE INTO customers (customer_id, customer_name, contact_email, contact_phone) VALUES (?, ?, ?, ?)";
        String insertarSQLDepartments = "INSERT OR IGNORE INTO departments (department_id, department_name, manager_id) VALUES (?, ?, ?)";
        String insertarSQLEmployeesRealistic = "INSERT OR IGNORE INTO employees_realistic (employee_id, first_name, last_name, department_id, hire_date, salary, position) VALUES (?, ?, ?, ?, ?, ?, ?)";
        String insertarSQLOrderItems = "INSERT OR IGNORE INTO order_items (order_item_id, order_id, product_name, quantity, price) VALUES (?, ?, ?, ?, ?)";
        String insertarSQLOrders = "INSERT OR IGNORE INTO orders (order_id, customer_id, order_date, amount) VALUES (?, ?, ?, ?)";
        String insertarSQLProjects = "INSERT OR IGNORE INTO projects (project_id, project_name, department_id, budget, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?)";


        try (BufferedReader br = new BufferedReader(new FileReader(archivoEmployeesProjects));
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

        try (BufferedReader br = new BufferedReader(new FileReader(archivoDepartments));
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

        try (BufferedReader br = new BufferedReader(new FileReader(archivoEmployeesRealistic));
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

        try (BufferedReader br = new BufferedReader(new FileReader(archivoOrderItems));
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

        try (BufferedReader br = new BufferedReader(new FileReader(archivoOrders));
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

        try (BufferedReader br = new BufferedReader(new FileReader(archivoProjects));
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

    public static void main(String[] args) throws FileNotFoundException {
        JFrame frame = new JFrame("Tabla");
        frame.setContentPane(new Tabla().JpanelP);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(1000, 515);
        frame.setResizable(false);
        frame.setVisible(true);
    }
}
