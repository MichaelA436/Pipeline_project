pipeline {
    agent any

    triggers {
        githubPush()
    }

    parameters {
        choice(
            name: 'RUN_MODE',
            choices: ['FULL', 'INCREMENTAL'],
            description: 'Choose ETL mode to run on Cloudera'
        )
    }

    environment {
        CLOUDERA_HOST = 'ec2-user@13.41.167.97'
        SSH_KEY       = '/var/lib/jenkins/.ssh/id_rsa'
        SPARK_SCRIPTS = '/tmp/michaelscripts/'
        PYTHON_BIN    = 'python3'
        JAVA_HOME = '/usr/lib/jvm/java-1.8.0'
        PATH = "${JAVA_HOME}/bin:${env.PATH}"
    }


    stages {

        stage('Debug Java') {
            steps {
                sh '''
        echo 'JAVA_HOME = $JAVA_HOME'
        which java
        java -version
        '''
            }
        }

        /* ---------------------------------------------------------
           1. Run tests locally (NO Spark here)
        --------------------------------------------------------- */
        stage('Run Tests (Local)') {
            steps {
                sh '''
                    ${PYTHON_BIN} --version

                    ${PYTHON_BIN} -m pip install --upgrade pip setuptools wheel
                    ${PYTHON_BIN} -m pip install "pytest>=6.2,<8"

                    if [ -f requirements.txt ]; then
                        ${PYTHON_BIN} -m pip install -r requirements.txt
                    fi

                    ${PYTHON_BIN} -m pytest tests/ -k "not spark"
                '''
            }
        }

        

        /* ---------------------------------------------------------
           2. Validate Cloudera cluster health
        --------------------------------------------------------- */
        stage('Validate Cloudera Cluster') {
            steps {
                sh '''
                    echo "=== Checking Cloudera Cluster Health ==="
                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} "
                        HADOOP_CONF_DIR=/etc/hadoop/conf yarn node -list 2>&1 | grep -E 'Total|RUNNING'
                        hdfs dfsadmin -report 2>&1 | grep -E 'Configured|DFS Remaining'
                        spark-submit --version 2>&1 | grep version
                    "
                '''
            }
        }

        /* ---------------------------------------------------------
           3. Copy ETL scripts to Cloudera
        --------------------------------------------------------- */
        stage('Copy ETL Scripts to Cluster') {
            steps {
                sh '''
                    echo "=== Copying ETL Scripts to Cloudera ==="

                    ssh -i ${SSH_KEY} -o StrictHostKeyChecking=no ${CLOUDERA_HOST} \
                        "mkdir -p ${SPARK_SCRIPTS}"

                    scp -i ${SSH_KEY} -o StrictHostKeyChecking=no \
                        full_load.py \
                        silver_cleaning2.py \
                        updated_incremental_load.py \
                        transformation.py \
                        ${CLOUDERA_HOST}:${SPARK_SCRIPTS}/
                '''
            }
        }

        /* ---------------------------------------------------------
           4. FULL LOAD (only when RUN_MODE == FULL)
        --------------------------------------------------------- */
        stage('Full Load') {
            when { expression { params.RUN_MODE == 'FULL' } }
            steps {
                sh '''
                    echo "=== Running FULL LOAD on Cloudera ==="
                    ssh -i ${SSH_KEY} ${CLOUDERA_HOST} "
                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export YARN_CONF_DIR=/etc/hadoop/conf

                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          ${SPARK_SCRIPTS}/full_load.py
                    "
                '''
            }
        }

        /* ---------------------------------------------------------
           5. INCREMENTAL LOAD (only when RUN_MODE == INCREMENTAL)
        --------------------------------------------------------- */
        stage('Incremental Load') {
            when { expression { params.RUN_MODE == 'INCREMENTAL' } }
            steps {
                sh '''
                    echo "=== Running INCREMENTAL LOAD on Cloudera ==="
                    ssh -i ${SSH_KEY} ${CLOUDERA_HOST} "
                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export YARN_CONF_DIR=/etc/hadoop/conf
                        
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          ${SPARK_SCRIPTS}/updated_incremental_load.py
                    "
                '''
            }
        }

        /* ---------------------------------------------------------
           6. CLEANING (always runs after whichever load was chosen)
        --------------------------------------------------------- */
        stage('Cleaning') {
            steps {
                sh '''
                    echo "=== Running CLEANING job on Cloudera ==="
                    ssh -i ${SSH_KEY} ${CLOUDERA_HOST} "
                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export YARN_CONF_DIR=/etc/hadoop/conf
                        
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          ${SPARK_SCRIPTS}/silver_cleaning2.py
                    "
                '''
            }
        }

        /* ---------------------------------------------------------
           7. TRANSFORMATION (always last)
        --------------------------------------------------------- */
        stage('Transformation') {
            steps {
                sh '''
                    echo "=== Running TRANSFORMATION job on Cloudera ==="
                    ssh -i ${SSH_KEY} ${CLOUDERA_HOST} "
                        export HADOOP_CONF_DIR=/etc/hadoop/conf
                        export YARN_CONF_DIR=/etc/hadoop/conf
                        
                        spark-submit \
                          --master yarn \
                          --deploy-mode client \
                          ${SPARK_SCRIPTS}/transformation.py
                    "
                '''
            }
        }

        /* ---------------------------------------------------------
           8. Check YARN results
        --------------------------------------------------------- */
        stage('Check YARN Result') {
            steps {
                sh '''
                    echo "=== Recent YARN Applications ==="
                    ssh -i ${SSH_KEY} ${CLOUDERA_HOST} \
                        "yarn application -list -appStates FINISHED | head -10"
                '''
            }
        }
    }

    post {
        success { echo 'ETL pipeline completed successfully on Cloudera YARN.' }
        failure { echo 'Pipeline failed. Check logs.' }
    }
}
