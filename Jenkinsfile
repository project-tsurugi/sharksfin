pipeline {
    triggers { pollSCM('H/5 * * * *') }
    agent {
        docker {
            image 'nautilus/oltp-sandbox'
            label 'docker'
        }
    }
    environment {
        GITHUB_URL = 'https://github.com/project-tsurugi/sharksfin'
        BUILD_PARALLEL_NUM="8"
    }
    stages {
        stage ('Prepare env') {
            steps {
                sh '''
                    ssh-keyscan -t rsa github.com > ~/.ssh/known_hosts
                '''
            }
        }
        stage ('checkout master') {
            steps {
                checkout scm
                sh '''
                    git clean -dfx
                    git submodule update --init --recursive
                '''
            }
        }
        stage ('Install foedus') {
            steps {
                sh '''
                    cd third_party/foedus
                    git log -n 1 --format=%H
                    # git clean -dfx
                    mkdir -p build
                    cd build
                    rm -f CMakeCache.txt
                    cmake -DCMAKE_BUILD_TYPE=Debug -DGFLAGS_INTTYPES_FORMAT=C99 -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/.local ..
                    make all install -j${BUILD_PARALLEL_NUM}
                '''
            }
        }
        stage ('Build') {
            steps {
                sh '''
                    mkdir build
                    cd build
                    cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_FOEDUS_BRIDGE=ON -DENABLE_COVERAGE=ON -DCMAKE_PREFIX_PATH=${WORKSPACE}/.local -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
                    make all -j${BUILD_PARALLEL_NUM}
                '''
            }
        }
        stage ('Test') {
            environment {
                GTEST_OUTPUT="xml"
            }
            steps {
                sh '''
                    cd build
                    make test ARGS="--verbose"
                '''
            }
        }
        stage ('Doc') {
            steps {
                sh '''
                    cd build
                    make doxygen > doxygen.log 2>&1
                    zip -q -r sharksfin-doxygen doxygen/html
                '''
            }
        }
        stage ('Coverage') {
            environment {
                GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e .*/antlr.*'
            }
            steps {
                sh '''
                    cd build
                    mkdir gcovr-xml gcovr-html
                    gcovr -j ${BUILD_PARALLEL_NUM} -r .. --xml ${GCOVR_COMMON_OPTION} -o gcovr-xml/sharksfin-gcovr.xml
                    gcovr -j ${BUILD_PARALLEL_NUM} -r .. --html --html-details --html-title "sharksfin coverage" ${GCOVR_COMMON_OPTION} -o gcovr-html/sharksfin-gcovr.html
                    zip -q -r sharksfin-coverage-report gcovr-html
                '''
            }
        }
        stage ('Lint') {
            steps {
                sh '''#!/bin/bash
                    python tools/bin/run-clang-tidy.py -quiet -export-fixes=build/clang-tidy-fix.yaml -p build -header-filter=$(pwd)'/([^./]+/)?(include|src)/([^./]+/)*[^./]+\\.h$' $(pwd)'/([^./]+/)?src/.*' > build/clang-tidy.log 2> build/clang-tidy-error.log
                '''
            }
        }
        stage ('Graph') {
            steps {
                sh '''
                    cd build
                    cp ../cmake/CMakeGraphVizOptions.cmake .
                    cmake --graphviz=dependency-graph/sharksfin.dot ..
                    cd dependency-graph
                    dot -T png sharksfin.dot -o sharksfin.png
                '''
            }
        }
    }
    post {
        always {
            junit keepLongStdio: true, allowEmptyResults: true, testResults: '**/*_gtest_result.xml'
            warnings consoleParsers: [
                [parserName: 'GNU Make + GNU C Compiler (gcc)'],
            ]
            warnings parserConfigurations: [
                [parserName: 'Clang (LLVM based)', pattern: 'build/clang-tidy.log'],
            ], unstableTotalAll: '1'
            warnings parserConfigurations: [
                [parserName: 'Doxygen', pattern: 'build/doxygen.log'],
            ]
            cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'build/gcovr-xml/sharksfin-gcovr.xml', failNoReports: false, failUnhealthy: false, failUnstable: false, maxNumberOfBuilds: 0, zoomCoverageChart: false
            archiveArtifacts allowEmptyArchive: true, artifacts: 'build/sharksfin-coverage-report.zip, build/sharksfin-doxygen.zip, build/clang-tidy-fix.yaml, build/dependency-graph/sharksfin.png', onlyIfSuccessful: true
            openTasks high: 'FIXME', normal: 'TODO', pattern: '**/*.md,**/*.txt,**/*.in,**/*.cmake,**/*.cpp,**/*.h', excludePattern: 'third_party/**'
            notifySlack('shakujo-dev')
        }
    }
}
