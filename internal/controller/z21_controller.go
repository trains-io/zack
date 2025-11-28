/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nats-io/nats.go"

	zackv1alpha1 "github.com/trains-io/zack/api/v1alpha1"
	"github.com/trains-io/zack/internal/common"
)

// Z21Reconciler reconciles a Z21 object
type Z21Reconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	NATS        *nats.Conn
	subscribers sync.Map
}

type StatusMsg struct {
	Reachable bool   `json:"reachable"`
	Serial    string `json:"serial"`
	TS        string `json:"ts"`
}

// +kubebuilder:rbac:groups=zack.trains.io,resources=z21s,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zack.trains.io,resources=z21s/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=zack.trains.io,resources=z21s/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Z21 object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *Z21Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)
	logger.Info("Starting reconcile", "controller", "Z21Reconciler")

	var z21 zackv1alpha1.Z21
	if err := r.Get(ctx, req.NamespacedName, &z21); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.ensureDeployment(ctx, &z21); err != nil {
		logger.Error(err, "unable to create or update Deployment")
		return ctrl.Result{}, err
	}

	topic := fmt.Sprintf("z21.%s.status", z21.Name)
	if _, exists := r.subscribers.Load(topic); !exists {
		logger.Info("Subscribing to NATS topic", "topic", topic)

		sub, err := r.NATS.Subscribe(topic, func(msg *nats.Msg) {
			var s StatusMsg
			if err := json.Unmarshal(msg.Data, &s); err != nil {
				logger.Error(err, "failed to unmarschal status message")
				return
			}

			logger.Info("Received status update", "topic", topic, "reachable", s.Reachable)

			go func() {
				ctx := context.Background()
				var z21obj zackv1alpha1.Z21
				if err := r.Get(ctx, req.NamespacedName, &z21obj); err != nil {
					logger.Error(err, "failed to get Z21 for status update")
					return
				}

				z21obj.Status.Reachable = s.Reachable
				if err := r.Status().Update(ctx, &z21obj); err != nil {
					logger.Error(err, "failed to update Z21 status")
				}
			}()
		})

		if err != nil {
			logger.Error(err, "failed to update Z21 status")
			return ctrl.Result{}, err
		}

		r.subscribers.Store(topic, sub)
	}

	//logger.Info("Deployment reconciled", "name", deploy.Name)
	return ctrl.Result{}, nil
}

// Helper for pointer values
func ptr[T any](v T) *T { return &v }

func (r *Z21Reconciler) ensureDeployment(ctx context.Context, z21 *zackv1alpha1.Z21) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("z21-gateway-%s", z21.Name),
			Namespace: z21.Namespace,
		},
	}

	desired := appsv1.Deployment{
		ObjectMeta: deploy.ObjectMeta,
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(int32(1)),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": fmt.Sprintf("z21-gateway-%s", z21.Name)},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": fmt.Sprintf("z21-gateway-%s", z21.Name)},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "z21-gateway",
							Image: "ghcr.io/trains-io/z21-gateway:v0.1.6",
							Env: []corev1.EnvVar{
								{Name: "Z21_NAME", Value: z21.Name},
								{Name: "Z21_ADDR", Value: z21.Spec.URL},
								{Name: "NATS_URL", Value: common.NATSURL},
							},
						},
					},
				},
			},
		},
	}

	_, err := ctrl.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		deploy.Spec = desired.Spec
		return controllerutil.SetControllerReference(z21, deploy, r.Scheme)
	})
	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *Z21Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zackv1alpha1.Z21{}).
		Owns(&appsv1.Deployment{}).
		Named("z21").
		Complete(r)
}
