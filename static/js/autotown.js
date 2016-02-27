
autotown = angular.module('autotown', ['ngRoute']).
    filter('relDate', function() {
        return function(dstr) {
            return moment(dstr).fromNow();
        };
    }).
    filter('agecss', function() {
        return function(dstr) {
            if (moment(dstr).diff(moment(), 'days') < -14) {
                return 'old'
            } else if (moment(dstr).diff(moment()) > 0) {
                return 'unavailable';
            }
            return '';
        };
    }).
    filter('calDate', function() {
        return function(dstr) {
            return moment(dstr).calendar();
        };
    }).
    filter('tomillis', function() {
        return function(seconds) {
            return (seconds * 1000).toFixed(2);
        };
    }).
    config(['$routeProvider', '$locationProvider',
            function($routeProvider, $locationProvider) {
                $locationProvider.html5Mode(true);
                $locationProvider.hashPrefix('!');

                $routeProvider.
                    when('/', {
                        templateUrl: '/static/partials/home.html',
                        controller: 'IndexCtrl'
                    }).
                    when('/tune/:tuna', {
                        templateUrl: '/static/partials/tune.html',
                        controller: 'TuneCtrl'
                    }).
                    when('/crash/', {
                        templateUrl: '/static/partials/crashes.html',
                        controller: 'CrashCtrl'
                    }).
                    otherwise({
                        redirectTo: '/'
                    });
            }]);

function isBogusTau(t) {
    return t < 0.0075 || t > .3;
}

autotown.controller('IndexCtrl', ['$scope', '$http',
                                  function($scope, $http) {
                                      $scope.isBogusTau = isBogusTau;

                                      $http.get("/api/recentTunes").success(function(data) {
                                          $scope.recentTunes = data;
                                          $scope.olderFun = function(d) {
                                              if (!d.older) {
                                                  return ""
                                              }
                                              var rv = [];
                                              for (var i = 0; i < d.older.length; i++) {
                                                  var x = d.older[i];
                                                  rv.push("" + (x.tau * 1000).toFixed(2) + "@" +
                                                          moment(x.timestamp).fromNow());

                                              }
                                              return rv.join(', ');
                                          };
                                          $scope.olderSparks = function(d) {
                                              if (!d.older) return "";
                                              var rv = [];
                                              for (var i = 0; i < d.older.length; i++) {
                                                  var x = d.older[i];
                                                  rv.push((x.tau * 1000).toFixed(2));

                                              }
                                              rv.reverse();
                                              return "";
                                          };

                                          setTimeout(function() {
                                              for (var i = 0; i < data.length; i++) {
                                                  var d = data[i];
                                                  if (!d.older) { continue; }
                                                  var rv = [];
                                                  for (var j = 0; j < d.older.length; j++) {
                                                      var x = d.older[j];
                                                      rv.push((x.tau * 1000).toFixed(2));
                                                  }
                                                  rv.reverse();
                                                  rv.push((d.Tau * 1000).toFixed(2));
                                                  jQuery('#spark-' + d.Key).sparkline(rv);
                                              }
                                          }, 1);
                                      });
                                  }]);


autotown.controller('TuneCtrl', ['$scope', '$http', '$routeParams',
                                 function($scope, $http, $routeParams) {
                                     $scope.isBogusTau = isBogusTau;
                                     $scope.rawLink = "/api/tune?tune=" +
                                         encodeURIComponent($routeParams.tuna);
                                     $http.get($scope.rawLink).success(function(data) {
                                         $scope.tune = data;

                                         $scope.hw = {};
                                         var board = data.Orig.rawSettings["Hw" + data.Board].fields;
                                         for (var k in board) {
                                             if (k.match(/.*Rate/)) {
                                                 $scope.hw.mpurate = board[k];
                                             } else if (k.match(/.*Accel.*LPF/)) {
                                                 $scope.hw.accellpf = board[k];
                                             } else if (k.match(/.*Gyro.*LPF/)) {
                                                 $scope.hw.gyrolpf = board[k];
                                             } else if (k.match(/.*LPF/)) {
                                                 $scope.hw.accellpf = board[k];
                                                 $scope.hw.gyrolpf = board[k];
                                             }
                                         }

                                         var as = data.Orig.rawSettings.ActuatorSettings.fields;
                                         $scope.bterm = as.MotorInputOutputCurveFit;
                                         if (typeof $scope.bterm === 'object') {
                                             $scope.bterm = $scope.bterm[1];
                                         }
                                     });

                                     var relatedLink = "/api/relatedTunes?tune=" +
                                         encodeURIComponent($routeParams.tuna);
                                     $http.get(relatedLink).success(function(data) {
                                         $scope.related = data;
                                     })
                                 }]);

autotown.controller('CrashCtrl', ['$scope', '$http',
                                  function($scope, $http) {
                                      $http.get("/api/recentCrashes").success(function(data) {
                                          $scope.recentCrashes = data;
                                          $scope.crashServer = 'https://console.developers.google.com/m/cloudstorage/b/dronin-autotown.appspot.com/o/';
                                      });
                                  }]);
