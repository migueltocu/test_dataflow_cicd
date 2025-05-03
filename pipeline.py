import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.userstate import BagStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain
import json
import datetime

# Estado y Timer
RECURSOS_STATE = BagStateSpec("recursos", beam.coders.StrUtf8Coder())
RECURSO_TIMER = TimerSpec("recurso_timer", TimeDomain.REAL_TIME)

# Reglas según urgencia
REQUISITOS_URGENCIA = {
    "baja": ["policia"],
    "media": ["policia", "ambulancia"],
    "alta": ["policia", "ambulancia", "bombero"]
}

def distancia(a, b):
    return abs(a['lat'] - b['lat']) + abs(a['lon'] - b['lon'])

class MatchingDoFn(beam.DoFn):
    def process(self, element, recursos_state=beam.DoFn.StateParam(RECURSOS_STATE), recurso_timer=beam.DoFn.TimerParam(RECURSO_TIMER)):
        clave, valor = element
        data = json.loads(valor)

        if 'urgencia' in data:
            alerta = data
            recursos = [json.loads(r) for r in recursos_state.read()]
            tipos_necesarios = REQUISITOS_URGENCIA.get(alerta['urgencia'], [])

            asignados = []
            recursos_actualizados = []

            for tipo in tipos_necesarios:
                disponibles = [r for r in recursos if r['estado'] == 'disponible' and r['tipo'] == tipo]
                if disponibles:
                    elegido = min(disponibles, key=lambda r: distancia(r['ubicacion'], alerta['ubicacion']))
                    elegido['estado'] = 'ocupado'
                    elegido['alerta_id'] = alerta.get('id_alerta', 'desconocido')
                    asignados.append(elegido)

            if len(asignados) == len(tipos_necesarios):
                for asignado in asignados:
                    recursos_actualizados.append(json.dumps(asignado))
                    segundos = 10 if alerta['urgencia'] == 'baja' else 20 if alerta['urgencia'] == 'media' else 30
                    recurso_timer.set(datetime.datetime.utcnow() + datetime.timedelta(seconds=segundos))

                recursos_state.clear()
                for r in recursos:
                    if r['estado'] == 'ocupado' or r['id_recurso'] not in [a['id_recurso'] for a in asignados]:
                        recursos_actualizados.append(json.dumps(r))

                for r in recursos_actualizados:
                    recursos_state.add(r)

                yield json.dumps({
                    'alerta': alerta,
                    'recursos_asignados': asignados
                })
            else:
                yield beam.pvalue.TaggedOutput('no_match', json.dumps(alerta))
        else:
            recursos_state.add(json.dumps(data))

    @on_timer(RECURSO_TIMER)
    def liberar_recurso(self, recursos_state=beam.DoFn.StateParam(RECURSOS_STATE)):
        recursos = [json.loads(r) for r in recursos_state.read()]
        recursos_actualizados = []

        for r in recursos:
            if r['estado'] == 'ocupado':
                r['estado'] = 'disponible'
                r.pop('alerta_id', None)
            recursos_actualizados.append(json.dumps(r))

        recursos_state.clear()
        for r in recursos_actualizados:
            recursos_state.add(r)

def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--recursos_topic', required=True)
    parser.add_argument('--output_topic', required=False)
    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Lee alertas
        alertas = (p
                   | "Leer alertas" >> beam.io.ReadFromPubSub(topic=args.input_topic).with_output_types(bytes)
                   | "Decode alertas" >> beam.Map(lambda x: x.decode('utf-8'))
                   | "KV alertas" >> beam.Map(lambda x: ("clave_unica", x))
                  )

        # Lee recursos
        recursos = (p
                    | "Leer recursos" >> beam.io.ReadFromPubSub(topic=args.recursos_topic).with_output_types(bytes)
                    | "Decode recursos" >> beam.Map(lambda x: x.decode('utf-8'))
                    | "KV recursos" >> beam.Map(lambda x: ("clave_unica", x))
                   )

        entradas = (alertas, recursos) | "Merge entradas" >> beam.Flatten()

        resultados = (entradas
                      | "Aplicar matching" >> beam.ParDo(MatchingDoFn()).with_outputs('no_match', main='matches')
                     )

        (resultados.matches
         | "A string matches" >> beam.Map(lambda x: json.dumps(json.loads(x)))
         | "Publicar matchings" >> beam.io.WriteToPubSub(topic=args.output_topic)
        )

if __name__ == "__main__":
    run()