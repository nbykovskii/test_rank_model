import pandas as pd

import marfaLib as mrf


# Connections group
def connect_group() -> None:
    mrf.connect_to_ssh()
    mrf.connect_to_mysql()


def disconnect_group() -> None:
    mrf.disconnect_mysql()
    mrf.disconnect_ssh()


def get_data_group(sql_query) -> pd.DataFrame:
    dfs = mrf.run_mysql_query(sql_query)
    return pd.DataFrame(dfs)


# Metrics group
def union_id(row) -> str:
    return str(str(row['playerId']) + ':' + row['brandName'])


def average_dep_sum_eur(row) -> float:
    try:
        return row['depSumTotal'] / row['cntMonthWithDep']
    except ZeroDivisionError:
        return 0


def average_bet_sum_eur(row) -> float:
    try:
        return row['betSumTotal'] / row['cntMonthWithDep']
    except ZeroDivisionError:
        return 0


def average_cash_out_sum_eur(row) -> float:
    try:
        return row['cashoutSumTotal'] / row['cntMonthWithDep']
    except ZeroDivisionError:
        return 0


def average_ggr_eur(row) -> float:
    try:
        return row['GGRSumTotal'] / row['cntMonthWithDep']
    except ZeroDivisionError:
        return 0


def average_ngr_eur(row) -> float:
    try:
        return row['NGRSumTotal'] / row['cntMonthWithDep']
    except ZeroDivisionError:
        return 0


def bonus_effective(row) -> float:
    try:
        return (row['GGRSumTotal'] / row['cntMonthWithDep']) / (row['NGRSumTotal'] / row['cntMonthWithDep'])
    except ZeroDivisionError:
        return 0


def average_margin(row) -> float:
    try:
        return (row['GGRSumTotal'] / row['cntMonthWithDep']) / row['betSumTotal']
    except ZeroDivisionError:
        return 0


def wagering_factor(row) -> float:
    try:
        return row['betSumTotal'] / row['depSumTotal']
    except ZeroDivisionError:
        return 0


def ngr_hold(row) -> float:
    try:
        return row['NGRSumTotal'] / row['depSumTotal']
    except ZeroDivisionError:
        return 0


def w_div_d(row) -> float:
    try:
        return row['cashoutSumTotal'] / row['depSumTotal']
    except ZeroDivisionError:
        return 0


# Based and calculated metrics for score
def base_metrics_apply(df) -> pd.DataFrame:
    df['AverageDepSumEur'] = df.apply(average_dep_sum_eur, axis=1)
    df['AverageBetSumEur'] = df.apply(average_bet_sum_eur, axis=1)
    df['AverageCashOutSumEur'] = df.apply(average_cash_out_sum_eur, axis=1)
    df['AverageGgrSumEur'] = df.apply(average_ggr_eur, axis=1)
    df['AverageNgrSumEur'] = df.apply(average_ngr_eur, axis=1)

    return pd.DataFrame(df)


def calculate_metric_apply(df) -> pd.DataFrame:
    df['BonusEffective'] = df.apply(bonus_effective, axis=1)
    df['AverageMargin'] = df.apply(average_margin, axis=1)
    df['WageringFactor'] = df.apply(wagering_factor, axis=1)
    df['NgrHold'] = df.apply(ngr_hold, axis=1)
    df['WD'] = df.apply(w_div_d, axis=1)
    df['UnionID'] = df.apply(union_id, axis=1)

    return pd.DataFrame(df)


# Based metrics rank
def simple_rank(df) -> pd.DataFrame:
    df['TotalDepRank'] = df['depSumTotal'].rank(ascending=True, method='max', na_option='bottom', pct=True)
    df['TotalAmRank'] = df['AverageMargin'].rank(ascending=True, method='max', na_option='bottom', pct=True)
    df['TotalWfRank'] = df['WageringFactor'].rank(ascending=True, method='max', na_option='bottom', pct=True)
    df['TotalBeRank'] = df['BonusEffective'].rank(ascending=True, method='max', na_option='bottom', pct=True)

    return pd.DataFrame(df)


# Total ranking group
def total_scoring_krd(row) -> float:
    if row['GGRSumTotal'] > 0:
        return ((row['TotalDepRank'] * 55) ** 2) + ((row['TotalWfRank'] * 25) ** 2) + (
                    (row['TotalBeRank'] * 15) ** 2) + ((row['TotalAmRank'] * 5) ** 2)
    else:
        return ((row['TotalDepRank'] * 55) ** 2) + ((row['TotalWfRank'] * 25) ** 2) + (
                    (row['TotalWfRank'] * 15) ** 2) + ((row['TotalAmRank'] * 5) ** 2)


def total_scoring_krd_prct(df) -> pd.DataFrame:
    df['totalKrdPrct'] = round(df['totalKRD'].rank(ascending=True, method='max', na_option='bottom', pct=True), 2)

    return pd.DataFrame(df)


# Segmented
def numeric_segmented_group(row) -> int:
    if (round(float(row['totalKrdPrct']), 2) >= 0.01) & (round(float(row['totalKrdPrct']), 2) <= 0.11):
        return 1
    elif (round(float(row['totalKrdPrct']), 2) >= 0.12) & (round(float(row['totalKrdPrct']), 2) <= 0.39):
        return 2
    elif (round(float(row['totalKrdPrct']), 2) >= 0.40) & (round(float(row['totalKrdPrct']), 2) <= 0.53):
        return 3
    elif (round(float(row['totalKrdPrct']), 2) >= 0.54) & (round(float(row['totalKrdPrct']), 2) <= 0.76):
        return 4
    elif (round(float(row['totalKrdPrct']), 2) >= 0.77) & (round(float(row['totalKrdPrct']), 2) <= 0.86):
        return 5
    elif (round(float(row['totalKrdPrct']), 2) >= 0.87) & (round(float(row['totalKrdPrct']), 2) <= 0.92):
        return 6
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & (round(float(row['AverageDepSumEur']), 2) < 2500.0):
        return 7
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & ((round(float(row['AverageDepSumEur']), 2) >= 2500.0) & (row['AverageDepSumEur'] < 5000.0)):
        return 8
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & ((round(float(row['AverageDepSumEur']), 2) >= 5000.0) & (row['AverageDepSumEur'] < 10000.0)):
        return 9
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & (round(float(row['AverageDepSumEur']), 2) >= 10000.0):
        return 10
    else:
        return 0


def interp_segmented_group(row) -> str:
    if (round(float(row['totalKrdPrct']), 2) >= 0.01) & (round(float(row['totalKrdPrct']), 2) <= 0.11):
        return str('Bottom')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.12) & (round(float(row['totalKrdPrct']), 2) <= 0.39):
        return str('Very low')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.40) & (round(float(row['totalKrdPrct']), 2) <= 0.53):
        return str('Low')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.54) & (round(float(row['totalKrdPrct']), 2) <= 0.76):
        return str('Medium')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.77) & (round(float(row['totalKrdPrct']), 2) <= 0.86):
        return str('High')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.87) & (round(float(row['totalKrdPrct']), 2) <= 0.92):
        return str('Very High')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & (round(float(row['AverageDepSumEur']), 2) < 2500.0):
        return str('VIP 1')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & ((round(float(row['AverageDepSumEur']), 2) >= 2500.0) & (round(float(row['AverageDepSumEur']), 2) < 5000.0)):
        return str('VIP 2')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & ((round(float(row['AverageDepSumEur']), 2) >= 5000.0) & (round(float(row['AverageDepSumEur']), 2) < 10000.0)):
        return str('VIP 3')
    elif (round(float(row['totalKrdPrct']), 2) >= 0.93) & (round(float(row['AverageDepSumEur']), 2) >= 10000.0):
        return str('VIP 4')
    else:
        return str('Very Bottom')


if __name__ == '__main__':
    connect_group()

    # Base
    sql = "select * " \
          "from bireport_db.SourceRankedByUsers "

    df = get_data_group(sql)

    # Calc
    df_base = base_metrics_apply(df)
    df_calc = calculate_metric_apply(df_base)

    # Based Rank
    df_rank = simple_rank(df_calc)

    # Total Rank
    df_rank['totalKRD'] = df_rank.apply(total_scoring_krd, axis=1)
    df_prc_rank = total_scoring_krd_prct(df_rank).set_index('UnionID')

    # Segmentation
    df_prc_rank['NumericSegment'] = df_prc_rank.apply(numeric_segmented_group, axis=1)
    df_prc_rank['InterpSegment'] = df_prc_rank.apply(interp_segmented_group, axis=1)

    # Formatted
    df_format = df_prc_rank[['playerId', 'brandName', 'partnerId', 'partnerName',
                             'firstDepDay', 'lastDepDay', 'cntMonthWithDep', 'depSumTotal',
                             'AverageDepSumEur', 'AverageBetSumEur', 'AverageCashOutSumEur',
                             'AverageGgrSumEur', 'AverageNgrSumEur', 'BonusEffective', 'NgrHold', 'WD',
                             'totalKRD', 'totalKrdPrct', 'NumericSegment', 'InterpSegment']]

    # Uploading
    df_format.to_excel('./file/mvp_player_score.xlsx')

    # To DataBase
    # Test
    # Disconnect
    disconnect_group()
