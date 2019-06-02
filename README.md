# Sistema soporte decisiones

## About

TPO sistema de soprte de decisiones

## Setup DB

- Proceed to clone the repo.
- Make sure that you have java, docker & pentaho installed in your machine.
- Use the docker compose database or any PG11
  - `docker-compose up -d`

### Create local db

set the env vars:

- PG_UADE_BI_HOST
- PG_UADE_BI_USER
- PG_UADE_BI_PORT

`db/staging/create_db.sh`

## Using it

In order fire the transformations we need python3 installed (use conda for instance) and install the dependencies

```sh
conda activate py37 # if use conda
pip install -r src/etl/luigi/requirements.txt --no-index  --find-links file:/tmp/packages
PYTHONPATH='.' luigi --module src.etl.luigi.fetch_categoria Insert --local-scheduler
```

## Input

- Categoria
  - Id. de categoría
  - Nombre de categoría
  - Descripción
  - Imagen
- Clientes
  - Id. de cliente
  - Nombre de compañía
  - Nombre del contacto
  - Cargo del contacto
  - Dirección
  - Ciudad
  - Región
  - Código postal
  - País
  - Teléfono,Fax
- Compania envios
  - Id. de compañía de envíos
  - Nombre de compañía
  - Teléfono
- Detalle Pedidos
  - Id. de pedido
  - Producto
  - Precio por unidad
  - Cantidad
  - Descuento
- Empleados
  - Id. de empleado
  - Apellidos
  - Nombre
  - Cargo
  - Tratamiento
  - Fecha de nacimiento
  - Fecha de contratación
  - Dirección
  - Ciudad
  - Región
  - Código postal
  - País
  - Teléfono de domicilio
  - Extensión
  - Foto
  - Notas
  - Jefe
- Pedidos
  - Id. de pedido
  - Cliente
  - Empleado
  - Fecha de pedido
  - Fecha de entrega
  - Fecha de envío
  - Forma de envío
  - Cargo
  - Nombre de destinatario
  - Dirección de destinatario
  - Ciudad de destinatario
  - Región de destinatario
  - Código postal de destinatario
  - País de destinatario
- Productos
  - Id. de producto
  - Nombre de producto
  - Proveedor
  - Categoría
  - Cantidad por unidad
  - Precio por unidad
  - Unidades en existencia
  - Unidades pedidas
  - Nivel de nuevo pedido
  - Suspendido
- Proveedores
  - Id. de proveedor
  - Nombre de compañía
  - Nombre del contacto
  - Cargo del contacto
  - Dirección
  - Ciudad
  - Región
  - Código postal
  - País
  - Teléfono
  - Fax
  - Página principal

## Output

- dim_addresses
  - id
  - state
  - region
  - country
  - postal_code
- dim_categories
  - id
  - name
- dim_products
  - id
  - name
  - id_category
  - suspended
- dim_providers
  - id
  - name
  - id_address
