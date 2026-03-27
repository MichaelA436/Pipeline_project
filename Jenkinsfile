pipeline {
    agent any

    parameters {
        choice(
            name: 'RUN_MODE',
            choices: ['FULL', 'INCREMENTAL'],
            description: 'Choose whether to run full load or incremental load'
        )
    }

    environment {
        SPARK_HOME = "/usr/lib/spark"
        PYSPARK_PYTHON = "/usr/bin/python3"
        PATH = "$SPARK_HOME/bin:$PATH"
    }

    stages {

        stage('Full Load (Optional)') {
            when {
                expression { params.RUN_MODE == 'FULL' }
            }
            steps {
                echo "Running FULL LOAD..."
                sh "spark-submit --master local[*] full_load/full_load.py"
            }
        }

        stage('Cleaning After Full Load (Optional)') {
            when {
                expression { params.RUN_MODE == 'FULL' }
            }
            steps {
                echo 'Cleaning Bronze → Silver after FULL LOAD...'
                sh "spark-submit --master local[*] cleaning/cleaning.py"
            }
        }

        stage('Incremental Load (Optional)') {
            when {
                expression { params.RUN_MODE == 'INCREMENTAL' }
            }
            steps {
                echo "Running INCREMENTAL LOAD..."
                sh "spark-submit --master local[*] incremental/updated_incremental_load.py"
            }
        }

        stage('Cleaning After Incremental Load (Optional)') {
            when {
                expression { params.RUN_MODE == 'INCREMENTAL' }
            }
            steps {
                echo 'Cleaning Bronze → Silver after INCREMENTAL LOAD...'
                sh "spark-submit --master local[*] cleaning/cleaning.py"
            }
        }

        stage('Transformation (Optional)') {
            when {
                expression { params.RUN_MODE == 'INCREMENTAL' }
            }
            steps {
                echo "Running TRANSFORMATION (Silver → Gold)..."
                sh "spark-submit --master local[*] transformation/transformation.py"
            }
        }
    }

    post {
        success {
            echo "Pipeline completed successfully"
        }
        failure {
            echo "Pipeline failed — check logs"
        }
    }
}
