from sf_connection import ORG1
import pandas as pd
import argparse
from pandas.api.types import CategoricalDtype
from jinja2 import Environment, FileSystemLoader
import re
import datetime


class SalesforceData(object):
    #Just a little helper class to flatten the data

    def __init__(self, args):

        self.activityDate = args['Activity_Date__c']
        self.county = args['Engagement__r']['County__c']
        self.activity = args['Activity__c']
        self.owner = args['Assigned_To__c']

    def to_cat(self):
        return self.county, self.activity

def get_begin_and_end():

    # Reports should always be Sunday - Saturday, so figure out when those were
    today = datetime.date.today()
    begin = today - datetime.timedelta(days=(today.weekday()+1), weeks=1)
    end = begin + datetime.timedelta(days=6)

    return [begin, end]


def replace_strings(html_out):

    # Let's ditch some of the junk html and numbers for prettier things
    replaceables = { 'NaN': '0',
                     '\.0': '',
                     'DataFrame': 'table table-striped',
                     ' style="text-align: right;"': '',
                     ' border="1" class="dataframe"': '',
                     '<th>Activity</th>': '<th></th>',
                     '    <tr>\n\s+<th>Location</th>': '',
                     '<th></th>\n\s+</tr>': '',
                     '<th></th>': ''}

    for key in replaceables.keys():
        html_out = re.sub(key, replaceables[key], html_out)

    return html_out


def write_table(table, report_dates):

    # Grab the Jinja template and work with it.
    # Note: everything must be in and run from the CWD

    # TODO: generalize so everything doesn't have to be run from CWD...
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template("myreport.html")

    template_vars = {"title" : "Weekly Activity Report",
                     "beginDate" : str(report_dates[0]),
                     "endDate": str(report_dates[1]),
                     "Activity_Crosstab_Table": table.to_html()}


    html_out = template.render(template_vars)
    html_out = replace_strings(html_out)

    with open('report.html', 'w') as f:
        f.write(html_out)


def main(conn):

    report_dates = get_begin_and_end()

    query = 'SELECT Name,Activity__c,Assigned_To__c,Activity_Date__c,Engagement__r.Employer__c,Engagement__r.County__c FROM Outreach_Activity__c '
    query += 'WHERE Activity_Date__c >= {} AND Activity_Date__c <= {} ORDER BY Engagement__r.County__c'.format(str(report_dates[0]), str(report_dates[1]))

    data = conn.query(query)

    data = [ SalesforceData(record) for record in data['records']]


    counties = list(set([ result['Location_County__c'] for result in conn.query('SELECT Location_County__c  FROM Account GROUP BY Location_County__c')['records']]))
    activities = list(set([ result['Activity__c'] for result in conn.query('SELECT Activity__c FROM Outreach_Activity__c')['records']]))

    categorical_data = [data_point.to_cat() for data_point in data]

    df = pd.DataFrame(categorical_data, columns=['Location', 'Activity'], dtype='category')


    # This is the work around to add the missing data points
    # - Pandas only allows 1D categoricalCtype, so we will split our 2D arrays into 2, 1D arrays
    county_series = df['Location'].astype(CategoricalDtype(counties))
    activity_series = df['Activity'].astype(CategoricalDtype(activities))

    # Reassign the data into the original 2D array
    df['Location'] = county_series
    df['Activity'] = activity_series

    # Create the table and write to disk
    table =  pd.crosstab(df.Location, df.Activity, margins=True, dropna=False)
    write_table(table, report_dates)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Tool to generate the Manning report')
    parser.add_argument('-p', '--password', help='OIT_lwd password to log into Salesforce', required=True)

    args = parser.parse_args()

    try:
        conn = ORG1(args.password)
    except Exception(e):
        print e

    main(conn)