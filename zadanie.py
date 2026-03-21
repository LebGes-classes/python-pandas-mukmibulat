import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, Union


def load_data(path: str) -> pd.DataFrame:
    """
    Загружает данные из Excel файла.

    Args:
        path: Путь к Excel файлу

    Returns:
        DataFrame с очищенными названиями колонок (удалены пробелы, приведены к нижнему регистру)
    """

    df = pd.read_excel(path)
    df.columns = df.columns.str.strip().str.lower()

    return df


def parse_date(value: Union[str, datetime, float]) -> Optional[datetime]:
    """
    Преобразует строку с датой в объект datetime, пробуя несколько форматов.

    Args:
        value: Значение даты для парсинга (строка, datetime или NaN)

    Returns:
        Объект datetime или None, если парсинг не удался
    """

    if pd.isna(value):
        return None

    value = str(value).strip()

    formats = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%b %d, %Y",
        "%B %d, %Y"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except:
            pass

    return None


def convert_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Преобразует указанные колонки с датами из строк в datetime.

    Args:
        df: Входной DataFrame с колонками дат

    Returns:
        DataFrame с преобразованными колонками дат
    """

    date_columns = [
        "install_date",
        "warranty_until",
        "last_calibration_date",
        "last_service_date"
    ]

    for col in date_columns:
        df[col] = df[col].apply(parse_date)

    return df


def normalize_status(status: Union[str, float]) -> str:
    """
    Приводит статусы оборудования к стандартизированным категориям.

    Args:
        status: Исходное значение статуса для нормализации

    Returns:
        Нормализованная строка статуса
    """

    if pd.isna(status):
        return "unknown"

    status = str(status).strip().lower()

    mapping = {
        "ok": "operational",
        "working": "operational",
        "op": "operational",
        "maintenance": "maintenance_scheduled",
        "maint_sched": "maintenance_scheduled",
        "planned": "planned_installation",
        "scheduled_install": "planned_installation",
        "broken": "faulty",
        "error": "faulty"
    }

    if status in mapping:
        return mapping[status]

    return status


def normalize_status_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Применяет нормализацию статусов к колонке status.

    Args:
        df: Входной DataFrame с колонкой status

    Returns:
        DataFrame с нормализованными значениями статусов
    """

    df["status"] = df["status"].apply(normalize_status)

    return df


def clean_uptime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Очищает и преобразует колонку процента аптайма в числовой формат.

    Args:
        df: Входной DataFrame с колонкой uptime_pct

    Returns:
        DataFrame с очищенной колонкой uptime_pct в числовом формате
    """

    df["uptime_pct"] = (
        df["uptime_pct"]
        .astype(str)
        .str.replace(",", ".")
    )

    df["uptime_pct"] = pd.to_numeric(df["uptime_pct"], errors="coerce")

    return df


def check_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Проверяет согласованность дат (дата калибровки не может быть раньше даты установки).

    Args:
        df: Входной DataFrame с колонками дат

    Returns:
        DataFrame с некорректными датами калибровки, замененными на None
    """

    for i, row in df.iterrows():
        install = row["install_date"]
        calibration = row["last_calibration_date"]

        if install and calibration:
            if calibration < install:
                df.at[i, "last_calibration_date"] = None

    return df


def filter_warranty(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Разделяет устройства на группы по гарантии (в гарантии и вне гарантии).

    Args:
        df: Входной DataFrame с колонкой warranty_until

    Returns:
        Кортеж из двух DataFrame: (устройства_в_гарантии, устройства_вне_гарантии)
    """

    today = datetime.today()
    in_warranty = df[df["warranty_until"] >= today]
    out_warranty = df[df["warranty_until"] < today]

    return in_warranty, out_warranty


def sort_by_calibration_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Сортирует устройства по дате калибровки.

    Args:
        df: Входной DataFrame с данными о калибровке

    Returns:
        DataFrame, отсортированный по дате калибровки
    """

    return df.sort_values("last_calibration_date", ascending=False)


def sort_by_issues(df: pd.DataFrame) -> pd.DataFrame:
    """
    Сортирует устройства по количеству проблем.

    Args:
        df: Входной DataFrame с данными о проблемах

    Returns:
        DataFrame, отсортированный по количеству проблем
    """

    return df.sort_values("issues_reported_12mo", ascending=False)


def clinics_with_problems(df: pd.DataFrame) -> pd.DataFrame:
    """
    Агрегирует количество проблем по клиникам и сортирует по убыванию.

    Args:
        df: Входной DataFrame с данными о клиниках и проблемах

    Returns:
        DataFrame с ID клиники, названием и общим количеством проблем, отсортированный по убыванию
    """

    result = (
        df.groupby(["clinic_id", "clinic_name"])
        .agg({
            "issues_reported_12mo": "sum"
        })
        .sort_values("issues_reported_12mo", ascending=False)
    )

    return result


def calibration_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создает отчет со сроками калибровки для всех устройств.

    Args:
        df: Входной DataFrame с данными о калибровке устройств

    Returns:
        DataFrame с колонками device_id, clinic_name, model, last_calibration_date
    """

    report = df[[
        "device_id",
        "clinic_name",
        "model",
        "last_calibration_date"
    ]]

    return report


def summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создает сводную таблицу с проблемами и аптаймом по клиникам и моделям.

    Args:
        df: Входной DataFrame с данными о клиниках, моделях и метриках

    Returns:
        Сводная таблица с суммой проблем и средним аптаймом по клиникам и моделям
    """

    pivot = pd.pivot_table(
        df,
        index=["clinic_name", "model"],
        values=[
            "issues_reported_12mo",
            "uptime_pct"
        ],
        aggfunc={
            "issues_reported_12mo": "sum",
            "uptime_pct": "mean"
        }
    )

    return pivot


def main() -> None:
    """
    Главная функция выполнения программы.
    """

    df = load_data("medical_diagnostic_devices_10000.xlsx")
    df = convert_dates(df)
    df = normalize_status_column(df)
    df = clean_uptime(df)
    df = check_dates(df)

    in_warranty, out_warranty = filter_warranty(df)
    clinics = clinics_with_problems(df)
    calibration = calibration_report(df)
    summary = summary_table(df)

    with pd.ExcelWriter("output.xlsx") as writer:
        sort_by_calibration_dates(df).to_excel(writer, sheet_name='calibration_dates')
        filter_warranty(df)[0].to_excel(writer, sheet_name='warranty')
        sort_by_issues(df).to_excel(writer, sheet_name='issues')
        clinics.to_excel(writer, sheet_name='clinics_problems')
        summary.to_excel(writer, sheet_name='summary_table')

    print("Устройств на гарантии:", len(in_warranty))
    print("Устройств вне гарантии:", len(out_warranty))
    print(f"\nОтчет сохранен в файл: output.xlsx")

    print("\nКлиники с наибольшим количеством проблем")
    print(clinics.head())

    print("\nОтчет по срокам калибровки")
    print(calibration.head())

    print("\nСводная таблица по клиникам и оборудованию")
    print(summary.head())


if __name__ == "__main__":
    main()