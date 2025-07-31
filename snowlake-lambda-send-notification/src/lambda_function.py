# -*- coding: utf-8 -*-
import json
import boto3
import os
import html


def lambda_handler(event, context):
    """
    Fonction Lambda qui traite les événements d'échec des jobs Glue et envoie des notifications par email.
    """
    # Extraction des informations de l'événement CloudWatch
    try:
        # Analyse de l'événement CloudWatch
        detail = event['detail']
        job_name = detail['jobName']
        job_run_id = detail['jobRunId']
        state = detail['state']
        error_message = detail.get('ErrorMessage', 'Aucun message d\'erreur disponible')

        # Vérification si c'est un état d'erreur qui nécessite une notification
        if state in ['FAILED', 'TIMEOUT', 'ERROR', 'STOPPED']:
            # Créer un client Glue pour obtenir plus d'informations sur le job
            glue_client = boto3.client('glue')

            # Obtenir l'URL de la console pour le job Glue (création de l'URL)
            region = os.environ['AWS_REGION']
            account_id = context.invoked_function_arn.split(":")[4]

            job_url = f"https://{region}.console.aws.amazon.com/gluestudio/home?region={region}#/job/{job_name}/run/{job_run_id}"

            # Obtenir plus de détails sur l'exécution du job si nécessaire
            try:
                job_run_details = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
                start_time = job_run_details['JobRun'].get('StartedOn', 'N/A')
                end_time = job_run_details['JobRun'].get('CompletedOn', 'N/A')
                allocated_capacity = job_run_details['JobRun'].get('AllocatedCapacity', 'N/A')
                execution_time = job_run_details['JobRun'].get('ExecutionTime', 'N/A')
                worker_type = job_run_details["JobRun"].get("WorkerType", "N/A")
                error_message = job_run_details["JobRun"].get("ErrorMessage", "Aucun message d\'erreur disponible")
                trigger_name = job_run_details["JobRun"].get("TriggerName", "N/A")

                # Information supplémentaire pour faciliter le débogage
                job_arguments = job_run_details['JobRun'].get('Arguments', {})

            except Exception as e:
                print(f"Erreur lors de la récupération des détails du job: {str(e)}")
                start_time = end_time = allocated_capacity = execution_time = 'N/A'
                job_arguments = {}

            # Formatage du sujet
            subject = f"ALERTE: Job Glue {job_name} en état {state}"

            # Formater les arguments du job pour l'affichage HTML
            args_html = ""
            for key, value in job_arguments.items():
                args_html += f"<tr><td>{html.escape(key)}</td><td>{html.escape(str(value))}</td></tr>"

            # Déterminer la couleur d'état pour le style
            state_color = {
                'FAILED': '#D32F2F',
                'TIMEOUT': '#F57C00',
                'ERROR': '#D32F2F',
                'STOPPED': '#FFC107'
            }.get(state, '#2196F3')

            # Version HTML du message
            html_message = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Alerte Job Glue</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
        }}
        .header {{
            background-color: {state_color};
            color: white;
            padding: 15px;
            text-align: center;
            border-radius: 5px 5px 0 0;
        }}
        .content {{
            padding: 20px;
            background-color: #f9f9f9;
            border: 1px solid #ddd;
        }}
        .section {{
            margin-bottom: 20px;
            padding: 15px;
            background-color: white;
            border-radius: 5px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .section-title {{
            font-size: 18px;
            font-weight: bold;
            margin-bottom: 10px;
            padding-bottom: 8px;
            border-bottom: 1px solid #eee;
            color: {state_color};
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        table, th, td {{
            border: 1px solid #ddd;
        }}
        th, td {{
            padding: 10px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        .error-box {{
            background-color: #FFF8F8;
            border-left: 4px solid #D32F2F;
            padding: 10px 15px;
            margin: 10px 0;
            overflow-x: auto;
        }}
        .button {{
            display: inline-block;
            padding: 10px 15px;
            background-color: #2196F3;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            margin-top: 10px;
        }}
        .footer {{
            margin-top: 20px;
            padding: 15px;
            text-align: center;
            font-size: 12px;
            color: #666;
        }}
        code {{
            white-space: pre-wrap;
            word-wrap: break-word;
            background-color: #f5f5f5;
            padding: 10px;
            display: block;
            font-family: monospace;
            font-size: 13px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h2>Alerte Job AWS Glue</h2>
    </div>
    <div class="content">
        <div class="section">
            <p>Bonjour,</p>
            <p>Le job Glue <strong>"{job_name}"</strong> est passé à l'état <span style="color:{state_color};font-weight:bold">{state}</span>.</p>
        </div>

        <div class="section">
            <div class="section-title">Détails du job</div>
            <table>
                <tr>
                    <th style="width:30%">Propriété</th>
                    <th>Valeur</th>
                </tr>
                <tr>
                    <td>Job Name</td>
                    <td>{html.escape(job_name)}</td>
                </tr>
                <tr>
                    <td>État</td>
                    <td><span style="color:{state_color};font-weight:bold">{state}</span></td>
                </tr>
                <tr>
                    <td>ID d'exécution</td>
                    <td>{html.escape(job_run_id)}</td>
                </tr>
                <tr>
                    <td>Heure de début</td>
                    <td>{html.escape(str(start_time))}</td>
                </tr>
                <tr>
                    <td>Heure de fin</td>
                    <td>{html.escape(str(end_time))}</td>
                </tr>
                <tr>
                    <td>Temps d'exécution</td>
                    <td>{html.escape(str(execution_time))} secondes</td>
                </tr>
                <tr>
                    <td>Capacité allouée</td>
                    <td>{html.escape(str(allocated_capacity))} DPU</td>
                </tr>
                <tr>
                    <td>Type de worker</td>
                    <td>{html.escape(str(worker_type))}</td>
                </tr>
                <tr>
                    <td>Nom du déclencheur</td>
                    <td>{html.escape(str(trigger_name))}</td>
                </tr>
                <tr>
                    <td>Compte AWS</td>
                    <td>{html.escape(account_id)}</td>
                </tr>
            </table>
            <p>
                <a href="{job_url}" class="button">Voir dans la console AWS</a>
            </p>
        </div>

        <div class="section">
            <div class="section-title">Message d'erreur</div>
            <div class="error-box">
                <code>{html.escape(error_message)}</code>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Arguments du job</div>
            <table>
                <tr>
                    <th>Clé</th>
                    <th>Valeur</th>
                </tr>
                {args_html}
            </table>
        </div>

        <div class="footer">
            <p>Ce message est généré automatiquement. Merci de ne pas y répondre.</p>
            <p>Système de monitoring AWS Glue</p>
        </div>
    </div>
</body>
</html>
            """

            # Version texte simple du message (pour les clients de messagerie qui ne prennent pas en charge l'HTML)
            text_message = f"""
Bonjour,

Le job Glue "{job_name}" est passé à l'état "{state}".

Détails du job:
--------------
URL Console: {job_url}
ID d'exécution: {job_run_id}
État: {state}
Heure de début: {start_time}
Heure de fin: {end_time}
Capacité allouée: {allocated_capacity} DPU
Temps d'exécution: {execution_time} secondes
Compte AWS: {account_id}
Worker Type: {worker_type}
Trigger Name: {trigger_name}

Message d'erreur:
---------------
{error_message}

Arguments du job:
---------------
{json.dumps(job_arguments, indent=2)}

Cordialement,
Système de monitoring AWS Glue
            """

            # Préparer le message au format JSON pour SNS
            message_json = {
                "default": text_message,
                "email": text_message,
                "email-json": text_message,
                "sms": f"Alerte: Job Glue {job_name} en état {state}. Consultez votre email pour plus de détails.",
                "http": json.dumps({"job": job_name, "state": state, "error": error_message}),
                "https": json.dumps({"job": job_name, "state": state, "error": error_message}),
                "email-html": html_message
            }

            # Envoi du message par SNS avec structure JSON
            sns_client = boto3.client('sns')
            topic_arn = os.environ['SNS_TOPIC_ARN']

            response = sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps(message_json),
                Subject=subject,
                MessageStructure='json'
            )

            print(f"Notification envoyée avec MessageId: {response['MessageId']}")
            return {
                'statusCode': 200,
                'body': json.dumps('Notification d\'erreur envoyée avec succès')
            }
        else:
            print(f"État du job {state} ne nécessite pas de notification")
            return {
                'statusCode': 200,
                'body': json.dumps('Aucune notification nécessaire pour cet état')
            }

    except Exception as e:
        print(f"Erreur lors du traitement de l'événement: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erreur: {str(e)}')
        }
