from airflow.models import Pool
from airflow.utils.session import create_session

from config.config import AIRFLOW_POOL_NAME_FOR_SPARK_CONCUR_ACCESS


def create_airflow_pools():
    with create_session() as session:
        pool_name = AIRFLOW_POOL_NAME_FOR_SPARK_CONCUR_ACCESS

        existing_pool = (
            session.query(Pool)
            .filter(Pool.pool == pool_name)
            .first()
        )

        if existing_pool is None:
            pool = Pool(
                pool=pool_name,
                slots=1,
                description="Limit concurrent Spark batch jobs",
                include_deferred=False,
            )
            session.add(pool)
            print(f"Pool '{pool_name}' created.")

        else:
            print(f"Pool '{pool_name}' already exists.")


if __name__ == "__main__":
    create_airflow_pools()