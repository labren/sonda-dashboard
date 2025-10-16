import pandas as pd

def testeTemporal(df, expected_rows, expected_interval, expected_last_time):

    # Coleta os timestamps da coluna 'timestamp' e calcula os intervalos
    time_diffs = df['timestamp'].diff().dropna()

    # Teste 1: Verifica se a quantidade de registros é a esperada
    if len(df) != expected_rows:
        if len(df) < expected_rows:
            return f"número de linhas menor que o esperado, esperado: {expected_rows}, encontrado: {len(df)}"
        else:
            return f"número de linhas maior que o esperado, esperado: {expected_rows}, encontrado: {len(df)}"

    # Teste 2: Verifica se o primeiro timestamp é o esperado
    elif df['timestamp'].iloc[-1].time() != expected_last_time:
        return f"último timestamp não é o esperado, esperado: {expected_last_time}, encontrado: {df['timestamp'].iloc[-1].time()}"

    # Teste 3: Teste de intervalo temporal: Verifica se o intervalo entre os timestamps é o esperado        
    elif not (time_diffs == expected_interval).all():
        # Identifica os índices onde o intervalo está fora do esperado
        idxs_problema = time_diffs[time_diffs != expected_interval].index.tolist()
        # Exibe os timestamps problemáticos usando strftime para detalhar
        timestamps_problema = df.loc[idxs_problema, 'timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
        return (
            f"intervalo temporal fora do esperado com intervalo de {expected_interval.total_seconds()} segundos, "
            f"timestamps problemáticos: {timestamps_problema}")
    else:
        return ""