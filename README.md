# Pipeline de Datos Financieros

Un pipeline completo de ingeniería de datos para datos financieros, que realiza procesamiento de datos tipo batch utilizando Apache Airflow, stream de datos real-time con Kafka, y almacenamiento centralizado en una base de datos PostgreSQL, cada servicio inicializado con Docker Compose.

## Overview

El proyecto demuestra un pipeline de ingeniería de datos que procesa datos tanto tipo batch como streaming, relacionados con el sector financiero:

- **Procesamiento Tipo Batch**: Historial de precio de un stock y fundamentos de la compañía correspondiente, procesados utilizando DAGs de Airflow
- **Procesamiento Tipo Streaming**: Cambios de precio en tiempo real, y ordenes de compra/venta utilizando Kafka
- **Almacenamiento**: Base de datos PostgreSQL para ambos tipos de datos
- **Infraestructura**: Docker Compose para la orquestación y manejo fáciles de los servicios.

## Architecture

![Architecture Diagram](architecture.png)

## Componentes

- **PostgreSQL**: DB central para almacenar todos los datos
- **Apache Airflow**: Orquestación de workflows para datos tipo batch
- **Apache Kafka**: Se encarga del streaming de los datos en tiempo real
- **Python**: Se utiliza para transformaciones y procesamiento de datos

## Fuentes de Datos

- **Batch**:
  - Historia de precio de los stocks (CSV)
  - Fundamentos de la compañía y apreciaciones de analistas (CSV)
  
- **Streaming**:
  - Cambios de precio en tiempo real
  - Órdenes de compra/venta

## License

This project is licensed under the MIT License - see the LICENSE file for details.
