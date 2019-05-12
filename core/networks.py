import torch
import torch.nn as nn
import torch.optim as opt
import matplotlib.pyplot as plt
import random


class OneLayer(nn.Module):
    """A neural network with exactly 1 hidden layer."""
    def __init__(self, num_inputs, num_hidden, num_out):
        super(OneLayer, self).__init__()

        self.layer1 = nn.Linear(num_inputs, num_hidden)
        self.layer2 = nn.Linear(num_hidden, num_out)

    def forward(self, X):
        h_out = torch.sigmoid(self.layer1(X))
        y_pred = torch.sigmoid(self.layer2(h_out))
        return y_pred


class Util:
    """A class that contains all the utilities needed for training and
    evaluating our model."""

    def train(X_train, y_train, model, loss_fn, optimizer):
        """Trains our neural network model."""
        model.train()
        outputs = model(X_train)
        loss = loss_fn(outputs, y_train)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        return loss.item()

    def validate(X_v, y_v, model, loss_fn, optimizer):
        """Computes the loss for model using the validation data."""
        model.eval()
        y_out = model(X_v)
        loss = loss_fn(y_out, y_v)
        return loss.item()

    def accuracy(X_test, y_actual, model):
        """Computes the accuracy for our model using the test data."""
        model.eval()
        y_pred = model(X_test)
        combo = list(zip(y_pred.tolist(), y_actual.tolist()))
        combo = [(c[0][0], c[1][0]) for c in combo]
        correct = 0
        for pair in combo:
            true_pos = pair[0] >= 0.5 and pair[1] == 1.0
            true_neg = pair[0] < 0.5 and pair[1] == 0.0
            if true_pos or true_neg:
                correct += 1
        return correct/len(combo)

    def plot_stats(total_epochs, tl, vl, acc):
        """Uses matplotlib to show the training losses, validation losses,
        and validation accuracy over time."""
        iters = [i for i in range(total_epochs)]
        plt.plot(iters, tl, '-b', label='Training Loss')
        plt.plot(iters, vl, '-r', label='Validation Loss')
        plt.legend(loc="upper left")
        plt.figure()
        plt.plot(iters, acc, '-g', label='Accuracy')
        plt.show()
        return

    def split_xy(raw_data):
        """Removes the last column of raw data (a list of lists) and returns the X
        and y data as two separate lists in a pair."""
        X_data = [i[:-1] for i in raw_data]
        y_data = [[i[-1]] for i in raw_data]
        return X_data, y_data

    def partition_data(X_data, y_data, num_data):
        """Partitions the data into training, validation, and testing sets.
        Returns a tuple that contains X, y data for each."""
        num_train = int(0.8 * num_data)
        num_validate = int(0.8 * (num_data - num_train))
        test_idx = num_train + num_validate

        X_train = torch.tensor(X_data[:num_train], dtype=torch.float)
        y_train = torch.tensor(y_data[:num_train], dtype=torch.float)
        X_v = torch.tensor(X_data[num_train:test_idx], dtype=torch.float)
        y_v = torch.tensor(y_data[num_train:test_idx], dtype=torch.float)
        X_test = torch.tensor(X_data[test_idx:], dtype=torch.float)
        y_test = torch.tensor(y_data[test_idx:], dtype=torch.float)

        return X_train, y_train, X_v, y_v, X_test, y_test


def train_model(total_epochs, learning_rate, model, learning_data):
    random.shuffle(learning_data)

    X_data, y_data = Util.split_xy(learning_data)
    X_train, y_train, X_v, y_v, X_test, y_test = Util.partition_data(
        X_data,
        y_data,
        len(learning_data)
    )

    loss_fn = nn.BCELoss()
    optimizer = opt.SGD(model.parameters(), lr=learning_rate, momentum=0.9)

    training_losses = []
    validation_losses = []
    accuracies = []

    for epoch in range(total_epochs):
        tl = Util.train(X_train, y_train, model, loss_fn, optimizer)
        vl = Util.validate(X_v, y_v, model, loss_fn, optimizer)
        acc = Util.accuracy(X_v, y_v, model)
        training_losses.append(tl)
        validation_losses.append(vl)
        accuracies.append(acc)
        log_str = "Epoch: {}/{}, Training Loss: {}, Validation Loss: {}, Accuracy: {}"\
            .format(epoch, total_epochs, tl, vl, acc)
        print(log_str)

    Util.plot_stats(
        total_epochs,
        training_losses,
        validation_losses,
        accuracies
    )
