from datetime import datetime, timedelta
from itertools import chain
from os import path as op

import PyPDF2
import pandas
import tabula


class ScheduleReader:
    # Python friendly column names
    _headers = ['job_number', 'description', 'super', 'mix_description', 'shift', *range(16)]
    # Date formatters
    _publish_date_fmt = '%m/%d/%Y'
    _week_date_fmt = '%b %d'

    def __init__(self, path):
        self._path = path
        self._raw = None
        self._publish_date = None
        self._start_date = None
        self._end_date = None
        self._weeks = None
        self._df = None

        self.clean_dataframe()

    def clean_dataframe(self):
        self.add_name_column()
        self.correct_date_headers()

    def parse_date_strings(self, *date_strings):
        week_strings = chain.from_iterable(y.split('to') for y in date_strings)
        week_dates = map(lambda x: datetime.strptime(x.strip(), self._week_date_fmt), week_strings)
        dates_corrected = map(lambda x: x.replace(year=self.publish_date.year), week_dates)
        self._weeks = list(dates_corrected)
        self._start_date = self._weeks[0] - timedelta(days=2)
        self._end_date = self._weeks[-1] + timedelta(days=2)

    def parse_df(self):
        with open(self._path, 'rb') as fobj:
            _df = tabula.read_pdf(op.abspath(self._path), guess=False)[0]
            self.parse_date_strings(_df.columns[2], _df.columns[5])
            _df.iloc[:, 5:].fillna(False, inplace=True)
            _df.iloc[:, 5:].replace('X', True, inplace=True)
            _df.columns = self._headers
            _df = _df[1:]
            return _df

    @property
    def publish_date(self):
        if not self._publish_date:
            self._publish_date = datetime.strptime(self.raw.splitlines()[0], self._publish_date_fmt)
        return self._publish_date

    @property
    def raw(self):
        if not self._raw:
            pdf = PyPDF2.PdfFileReader(self._path)
            page = pdf.getPage(0)
            self._raw = page.extractText()
        return self._raw

    @property
    def employee_indexes(self):
        return self.df.index[self.df['job_number'].isnull() & self.df['shift'].isnull() & self.df['super'].isnull()]

    @property
    def employee_index_ranges(self):
        indexes = [range(y, self.employee_indexes[x + 1]) for x, y in enumerate(self.employee_indexes[:-1])]
        indexes.append(range(self.employee_indexes[-1], self.df.shape[1]))
        return indexes

    def correct_date_headers(self):
        date_range = pandas.period_range(self._start_date, self._end_date, freq='D')
        self.df.columns.values[5:-1] = date_range

    def add_name_column(self):
        self.df = self.df.assign(name='hello')
        for idx, index in enumerate(self.employee_index_ranges):
            self.df.loc[list(self.employee_index_ranges[idx]), 'name'] = self.employee_names.iloc[idx]
        self.df.drop(self.employee_indexes, inplace=True)

    @property
    def employee_names(self):
        return self.df.loc[self.employee_indexes]['description']

    @property
    def df(self):
        if self._df is None:
            self._df = self.parse_df()
        return self._df

    @df.setter
    def df(self, value):
        self._df = value

    def as_df(self):
        return self.df


def read_schedule(path):
    scheduler = ScheduleReader(path)
    return scheduler.as_df()
