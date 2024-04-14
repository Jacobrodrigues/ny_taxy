import pandas as pd
import gspread


def get_worksheet(sheet_id: str, service_account_file: str,
                  worksheet_name: str, **kwargs) -> pd.DataFrame:
    """Reads a worksheet from Google sheets using a service account.

    Args:
        sheet_id: the Google sheet id.
        service_account_file: the path to the service account file.
        worksheet_name: the name of the worksheet.
        kwargs: kwargs for gspread.models.Worksheet.get_all_records.

    Returns:
        A Pandas DataFrame with the worksheet data.
        The first line of the worksheet will be used as the header.
    """
    
    gc = gspread.service_account(service_account_file)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    df = pd.DataFrame(ws.get_all_records(**kwargs))

    return df


credentials = '/Users/raimundo.rodrigues/Desktop/pyairbyte/elt/client_secret_msd.json'

CONFIG = {
    'cadastros': {
        'get_worksheet': {
            'sheet_id': '1miHs01OPIixgfk3DSSsUAOYNGjWizZUqLAsQQeLaxoY',
            'service_account_file': credentials,
            'worksheet_name': 'Parceiros_Externos',
}}}


for name, config in CONFIG.items():
    print('-------------------------------------')
    print(f'Starting {name}')
    print('-------------------------------------')
    df = get_worksheet(**config['get_worksheet'])