class MaquinaEstados:
    NORMAL = 'NORMAL'
    ATENCAO = 'ATENÇÃO'
    CRITICO = 'CRÍTICO'

    _NIVEIS = {NORMAL: 0, ATENCAO: 1, CRITICO: 2}
    AMOSTRAS_ESCALADA = 2
    AMOSTRAS_RECUPERACAO = 5

    @classmethod
    def classificar(cls, metricas, cpu_limit=None, ram_limit=None):
        cpu_limit = 85.0 if cpu_limit is None else float(cpu_limit)
        ram_limit = 85.0 if ram_limit is None else float(ram_limit)
        gpu = metricas.get('gpu_percent')
        vram = metricas.get('gpu_mem_percent')
        temperatura = metricas.get('gpu_temp_celsius')

        critico = (
            metricas['cpu_percent'] >= cpu_limit
            or metricas['ram_percent'] >= ram_limit
            or (gpu is not None and gpu >= 90)
            or (vram is not None and vram >= 90)
            or (temperatura is not None and temperatura >= 90)
        )
        if critico:
            return cls.CRITICO

        atencao = (
            (gpu is not None and gpu >= 70)
            or (vram is not None and vram >= 80)
            or (temperatura is not None and temperatura >= 75)
        )
        return cls.ATENCAO if atencao else cls.NORMAL

    @classmethod
    def avancar(cls, atual, candidato, contagem, alvo):
        if alvo == atual:
            return atual, None, 0, None

        if alvo == candidato:
            contagem += 1
        else:
            candidato = alvo
            contagem = 1

        if cls._NIVEIS[alvo] > cls._NIVEIS[atual]:
            necessario = cls.AMOSTRAS_ESCALADA
        else:
            necessario = cls.AMOSTRAS_RECUPERACAO

        if contagem >= necessario:
            return alvo, None, 0, (atual, alvo)
        return atual, candidato, contagem, None
