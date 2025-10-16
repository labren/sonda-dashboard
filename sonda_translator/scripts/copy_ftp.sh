#!/bin/bash

SRC="USUARIOn@IP:/mnt/ftp/restricted/coleta/"
DEST="../ftp/restricted/coleta/"
MAX_RETRIES=10

# Função para executar o rsync com tentativas
sync_files() {
    local attempt=1
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Tentativa $attempt de $MAX_RETRIES..."

        rsync -avW --progress --partial --timeout=300 --bwlimit=0 \
            --exclude='camera/' \
            --exclude='security/' \
            --exclude='*/camera/' \
            --exclude='*/security/' \
            --exclude='*.jpg' \
            --exclude='*.jpeg' \
            --exclude='*.JPG' \
            --exclude='*.png' \
            --exclude='*.gif' \
            --exclude='*.bmp' \
            --exclude='*.tiff' \
            --exclude='*.svg' \
            --exclude='*.webp' \
            --exclude='*.pdf' \
            --exclude='*.nc' \
            --prune-empty-dirs \
            --inplace --delete-delay \
            --info=progress2 "$SRC" "$DEST"

        if [ $? -eq 0 ]; then
            echo "Transferência concluída com sucesso!"
            return 0
        else
            echo "Falha na transferência. Tentando novamente em 10 segundos..."
            sleep 10
        fi
        ((attempt++))
    done
    echo "Falha após $MAX_RETRIES tentativas. Verifique a conexão e tente novamente."
    return 1
}

# Executa a sincronização
sync_files
