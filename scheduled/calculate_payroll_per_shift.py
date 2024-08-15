#!/usr/local/bin/python3

"""
Don't forget to give permissions for integration for each notion database.
"""

import logging
import os

from notion_client import Client

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID_SHIFTS = os.getenv("DATABASE_ID_SHIFTS")
DATABASE_ID_PAYROLL = os.getenv("DATABASE_ID_PAYROLL")

LOG_LEVEL = os.getenv("LOG_LEVEL", logging.INFO)

notion = Client(auth=NOTION_API_KEY)

logger = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)


def get_accepted_shifts():
    """Get accepted shifts from notion."""
    response = notion.databases.query(
        **{
            "database_id": DATABASE_ID_SHIFTS,
            "filter": {
                "property": "Status",
                "status": {
                    "equals": "Accepted",
                },
            },
        }
    )
    logger.debug("Got shifts from database %s", DATABASE_ID_SHIFTS)
    return response["results"]


def filter_hanled_employees(date: str, employees: list[dict[str, str]]) -> list[dict[str, str]]:
    """Filter already handled employee."""
    existing_rows = notion.databases.query(
        **{
            "database_id": DATABASE_ID_PAYROLL,
            "filter": {
                "and": [
                    {
                        "property": "Date",
                        "date": {
                            "equals": date,
                        },
                    },
                    {
                        "or": [
                            {
                                "property": "Employee",
                                "relation": {
                                    "contains": employee["id"],
                                },
                            }
                            for employee in employees
                        ],
                    },
                ],
            },
        }
    )
    handled_employee = [row["properties"]["Employee"]["relation"][0]["id"] for row in existing_rows["results"]]
    logging.debug("Got filtered employees from database %s", DATABASE_ID_PAYROLL)
    return filter(lambda e: e["id"] not in handled_employee, employees)


def get_employee_data(employee_id: str) -> (str, float):
    """Get employee"s rate from database."""
    try:
        employee_page = notion.pages.retrieve(employee_id)
        logger.debug("Got employee data for employee %s", employee_id)
        return (
            employee_page["properties"]["Name"]["title"][0]["text"]["content"],
            employee_page["properties"]["Rate"]["number"],
        )
    except (IndexError, KeyError):
        # Person not found
        logger.exception("Error on get employee data")
        return "", 0


def create_payroll_entry(employee_id, date, shift_id, employee_name, employee_rate):
    """Create row in payroll table."""
    request_data = {
        "parent": {"database_id": DATABASE_ID_PAYROLL},
        "properties": {
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": f"{employee_name} {date}",
                        },
                    },
                ],
            },
            "Employee": {
                "relation": [
                    {"id": employee_id},
                ],
            },
            "Date": {
                "date": {
                    "start": date,
                },
            },
            "Daily rate": {
                "number": employee_rate,
            },
            "Shift": {
                "relation": [
                    {"id": shift_id},
                ],
            },
        },
    }
    notion.pages.create(**request_data)
    logger.debug("Created payroll entry in database %s", DATABASE_ID_PAYROLL)


def update_shift_status(shift_id, date):
    notion.pages.update(
        **{
            "page_id": shift_id,
            "properties": {
                "Status": {
                    "status": {
                        "name": "Handled",
                    },
                },
                "Name": {
                    "title": [
                        {
                            "text": {
                                "content": date,
                            },
                        },
                    ],
                },
            },
        }
    )
    logger.debug("Updated shift data for shift %s", shift_id)


def main():
    logger.info("Started calculation")
    shifts = get_accepted_shifts()
    for shift in shifts:
        shift_id = shift["id"]
        date = shift["properties"]["Date"]["date"].get("start")
        employees = shift["properties"]["On-shift staff"]["relation"]
        if not employees:
            logger.warning("No employees set for shift. Can't calculate payroll")
            return

        for employee in filter_hanled_employees(date, employees):
            employee_name, employee_rate = get_employee_data(employee["id"])

            create_payroll_entry(employee["id"], date, shift_id, employee_name, employee_rate)
            logger.info("Handled %s", employee_name)

        update_shift_status(shift_id, date)

    logger.info("Finished calculation")


if __name__ == "__main__":
    main()
