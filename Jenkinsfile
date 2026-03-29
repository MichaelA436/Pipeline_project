pipeline {
    agent any

    environment {
        PYTHON_BIN = '/usr/bin/python3'
    }

    parameters {
        choice(
            name: 'RUN_MODE',
            choices: ['FULL', 'INCREMENTAL'],
            description: 'Choose which ETL mode to run'
        )
    }

    stages {

        stage('Setup Python Environment') {
            steps {
                sh '''
                    echo "Using Python binary: ${PYTHON_BIN}"
                    ${PYTHON_BIN} --version

                    ${PYTHON_BIN} -m venv venv
                    . venv/bin/activate

                    pip install --upgrade pip wheel
                    pip install -r requirements.txt
                '''
            }
        }

        stage('Run Pytests') {
            steps {
                sh '''
                    . venv/bin/activate
                    pytest --maxfail=1 --disable-warnings -q
                '''
            }
        }

        stage('Full Load') {
            when {
                expression { params.RUN_MODE == 'FULL' }
            }
            steps {
                sh '''
                    . venv/bin/activate
                    spark-submit full_load/full_load.py
                '''
            }
        }

        stage('Cleaning') {
            steps {
                sh '''
                    . venv/bin/activate
                    spark-submit cleaning/cleaning.py
                '''
            }
        }

        stage('Incremental Load') {
            when {
                expression { params.RUN_MODE == 'INCREMENTAL' }
            }
            steps {
                sh '''
                    . venv/bin/activate
                    spark-submit incremental/incremental_load.py
                '''
            }
        }

        stage('Transformation (Hive)') {
            steps {
                sh '''
                    . venv/bin/activate
                    spark-submit transformation/transformation.py
                '''
            }
        }
    }

    post {
        success {
            echo "Pipeline completed successfully."
        }
        failure {
            echo "Pipeline failed. Check logs and pytest results."
        }
    }
}
