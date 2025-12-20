import csv
import datetime
import decimal
import hashlib
import os
import re
import typing

from ..data_types import Fingerprint
from ..data_types import Transaction
from .base import ExtractorBase


def beanify_account(name: str) -> str:
    rst = re.sub(r"[^a-zA-Z0-9-_:]", "", name)
    rst = re.sub(r"[ \t\n\r\v\f]", " ", rst)
    rst = re.sub(r"[ ]{2,}", " ", rst)
    rst = rst.replace(" ", "-")
    return rst


def parse_date(date_str: str) -> datetime.date:
    parts = date_str.split("/")
    return datetime.date(int(parts[-1]), *(map(int, parts[:-1])))


def parse_to_decimal(number_str: str) -> decimal.Decimal:
    try:
        return decimal.Decimal(number_str)
    except (ValueError, decimal.InvalidOperation):
        pass

    return decimal.Decimal("0.0")


class FidelityExtractor(ExtractorBase):
    """Extractor for Fidelity CSV exports"""

    EXTRACTOR_NAME = "fidelity"
    DEFAULT_IMPORT_ID = "{{ file | as_posix_path }}:{{ reversed_lineno }}"
    DATE_FIELD = "Run Date"
    ALL_FIELDS = [
        DATE_FIELD,
        "Account",
        "Account Number",
        "Action",
        "Symbol",
        "Description",
        "Type",
        "Exchange Quantity",
        "Exchange Currency",
        "Currency",
        "Price",
        "Quantity",
        "Exchange Rate",
        "Commission",
        "Fees",
        "Accrued Interest",
        "Amount",
        "Settlement Date",
    ]

    def __init__(self, input_file):
        super().__init__(input_file)

        self.fieldnames = None
        self._row_count = 0

        if hasattr(self.input_file, "name"):
            self.filename = self.input_file.name
        else:
            try:
                self.filename = os.path.basename(self.input_file)
            except Exception:
                self.filename = self.input_file

        it = self._iter()
        self._row_count = len(list(it))

    def _create_reader(self) -> csv.DictReader:
        self.input_file.seek(os.SEEK_SET, 0)
        reader = csv.DictReader(
            self.input_file,
            fieldnames=self.ALL_FIELDS,
            restkey=None,
            restval=None,
            dialect="excel",
        )

        self.fieldnames = reader.fieldnames
        return reader

    def _iter(self) -> typing.Generator[dict, None, None]:
        reader = self._create_reader()

        for d in reader:
            date = d.get(self.DATE_FIELD, "")
            if re.match(r"^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$", date):
                try:
                    _ = parse_date(date)
                    yield d
                except ValueError:
                    pass

    def detect(self) -> bool:
        try:
            return self.fieldnames == self.ALL_FIELDS
        except Exception:
            pass
        return False

    def fingerprint(self) -> Fingerprint | None:
        # get first row
        it = self._iter()
        try:
            row = next(it)
        except StopIteration:
            return None

        hash = hashlib.sha256()
        for field in self.fieldnames:
            hash.update(row[field].encode("utf8"))

        date_value = parse_date(row["Run Date"])
        if not date_value:
            date_value = datetime.date(1970, 1, 1)
        return Fingerprint(
            starting_date=date_value,
            first_row_hash=hash.hexdigest(),
        )

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        it = self._iter()
        for i, row in enumerate(it):
            run_date = parse_date(row.get("Run Date", "01/01/1970"))

            # account
            source_account = beanify_account(row.get("Account", ""))

            # description of the transaction
            desc = row.get("Action", "")

            # date of the transaction
            date = run_date

            # date when the transaction posted
            post_date = run_date

            # description of the transaction provided by the bank
            bank_desc = row.get("Description", "")

            # ISO 4217 currency symbol
            currency = row.get("Currency", "")

            # status of the transaction
            t_type = row.get("Type", "")

            # transaction amount
            amount = parse_to_decimal(row.get("Amount", "0.0"))

            last_four_digits = row.get("Account Number", "")[-4:]

            yield Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=self.filename,
                lineno=i + 1,
                reversed_lineno=i - self._row_count,
                source_account=source_account,
                date=date,
                post_date=post_date,
                desc=desc,
                bank_desc=bank_desc,
                amount=amount,
                currency=currency,
                type=t_type,
                last_four_digits=last_four_digits,
                extra=row,
            )
