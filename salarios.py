import happybase
import pandas as pd

# Diccionario para reemplazar siglas con nombres completos
# Este diccionario permite convertir las abreviaturas en sus nombres completos
# para mayor claridad en las consultas y visualización de datos.
replacements = {
    'experience_level': {'MI': 'Middle', 'SE': 'Senior', 'EN': 'Entry-level', 'EX': 'Expert'},
    'employment_type': {'FT': 'Full Time', 'PT': 'Part Time', 'CT': 'Contract', 'FL': 'Freelance'}
}

# Paso 1: Conexión con HBase
# Nos conectamos al servidor HBase y verificamos la existencia de la tabla.
connection = happybase.Connection('localhost')
table_name = 'data_science_salaries'

# Verificar si la tabla ya existe
if table_name.encode() not in connection.tables():
    # Crear la tabla si no existe, definiendo las familias de columnas.
    families = {
        'personal': dict(),  # Información personal del empleado
        'employment': dict(),  # Detalles del empleo
        'salary': dict(),  # Información salarial
        'metadata': dict()  # Metadatos
    }
    connection.create_table(table_name, families)
    print(f"Tabla '{table_name}' creada exitosamente.")
else:
    print(f"La tabla '{table_name}' ya existe.")

def cargar_datos():
    """
    Carga los datos del dataset en la tabla HBase.

    Esta función lee un archivo CSV que contiene información sobre salarios en ciencia de datos.
    Luego, convierte cada fila en un formato compatible con HBase, asignando una clave única
    y cargando los datos organizados en familias de columnas.
    """
    table = connection.table(table_name)
    file_path = '/home/sepantojad/DataScience_salaries_2024.csv'
    data = pd.read_csv(file_path)

    for index, row in data.iterrows():
        row_key = f"row_{index}".encode()  # Clave de fila única
        table.put(row_key, {
            b'personal:experience_level': replacements['experience_level'][row['experience_level']].encode(),
            b'personal:job_title': row['job_title'].encode(),
            b'personal:employee_residence': row['employee_residence'].encode(),
            b'employment:employment_type': replacements['employment_type'][row['employment_type']].encode(),
            b'employment:remote_ratio': str(row['remote_ratio']).encode(),
            b'employment:company_location': row['company_location'].encode(),
            b'employment:company_size': row['company_size'].encode(),
            b'salary:salary': str(row['salary']).encode(),
            b'salary:salary_currency': row['salary_currency'].encode(),
            b'salary:salary_in_usd': str(row['salary_in_usd']).encode(),
            b'metadata:work_year': str(row['work_year']).encode()
        })
    print("Datos cargados exitosamente en HBase.")

def recorrer_tabla():
    """
    Recorrer y mostrar las primeras 10 filas de la tabla.

    Esta función realiza un escaneo completo de la tabla en HBase y muestra un máximo de
    10 filas, organizadas por sus claves de fila y valores. Es útil para obtener una vista
    general de los datos almacenados.
    """
    table = connection.table(table_name)
    print("\n=== Primeras 5 filas de la tabla ===")
    count = 0
    for key, data in table.scan():
        print(f"Row Key: {key.decode()}")
        for col, val in data.items():
            print(f"{col.decode()}: {val.decode()}")
        print("-" * 40)
        count += 1
        if count >= 5:
            break

def top_10_salarios():
    """
    Mostrar los 10 empleados con los salarios más altos.

    Esta función clasifica todos los empleados en la tabla por su salario en USD y
    muestra los 10 mejores, incluyendo el puesto y el nivel de experiencia.
    """
    table = connection.table(table_name)
    employees = []
    for key, data in table.scan():
        employees.append({
            'job_title': data[b'personal:job_title'].decode(),
            'experience_level': data[b'personal:experience_level'].decode(),
            'salary_usd': int(data[b'salary:salary_in_usd'].decode())
        })
    sorted_employees = sorted(employees, key=lambda x: x['salary_usd'], reverse=True)[:10]
    print("\n=== Top 10 empleados con los salarios más altos ===")
    for emp in sorted_employees:
        print(f"Puesto: {emp['job_title']}, Experiencia: {emp['experience_level']}, Salario USD: {emp['salary_usd']}")

def promedio_por_tipo_de_empleo():
    """
    Calcular y mostrar el promedio de salarios por tipo de empleo.

    Esta función calcula el promedio de los salarios en USD para cada tipo de empleo
    (por ejemplo, Full Time, Part Time, Contract, etc.).
    """
    table = connection.table(table_name)
    salary_sums = {}
    employment_counts = {}
    for key, data in table.scan():
        employment_type = data[b'employment:employment_type'].decode()
        salary_usd = int(data[b'salary:salary_in_usd'].decode())
        salary_sums[employment_type] = salary_sums.get(employment_type, 0) + salary_usd
        employment_counts[employment_type] = employment_counts.get(employment_type, 0) + 1
    print("\n=== Promedio de salarios por tipo de empleo ===")
    for emp_type in salary_sums:
        avg_salary = salary_sums[emp_type] / employment_counts[emp_type]
        print(f"{emp_type}: Promedio Salario USD: {avg_salary:.2f}")

def distribucion_por_tamanio_empresa():
    """
    Contar el número de empleados por tamaño de empresa.

    Esta función analiza la cantidad de empleados que trabajan en empresas pequeñas,
    medianas o grandes, basado en los datos almacenados en HBase.
    """
    table = connection.table(table_name)
    company_sizes = {}
    for key, data in table.scan():
        company_size = data[b'employment:company_size'].decode()
        company_sizes[company_size] = company_sizes.get(company_size, 0) + 1
    print("\n=== Distribución por tamaño de empresa ===")
    for size, count in company_sizes.items():
        print(f"Tamaño {size}: {count} empleados")

def salarios_por_remoto():
    """
    Promedio de salarios por nivel de trabajo remoto.

    Calcula y muestra el salario promedio en USD para cada nivel de trabajo remoto (0%, 50%, 100%).
    """
    table = connection.table(table_name)
    remote_salaries = {}
    remote_counts = {}
    for key, data in table.scan():
        remote_ratio = int(data[b'employment:remote_ratio'].decode())
        salary_usd = int(data[b'salary:salary_in_usd'].decode())
        remote_salaries[remote_ratio] = remote_salaries.get(remote_ratio, 0) + salary_usd
        remote_counts[remote_ratio] = remote_counts.get(remote_ratio, 0) + 1
    print("\n=== Promedio de salarios por nivel de trabajo remoto ===")
    for remote_ratio in remote_salaries:
        avg_salary = remote_salaries[remote_ratio] / remote_counts[remote_ratio]
        print(f"Trabajo Remoto {remote_ratio}%: Promedio Salario USD: {avg_salary:.2f}")

# Paso 3: Ejecutar operaciones
cargar_datos()
recorrer_tabla()
top_10_salarios()
promedio_por_tipo_de_empleo()
distribucion_por_tamanio_empresa()
salarios_por_remoto()

# Cerrar la conexión al final
connection.close()
