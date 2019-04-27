import torch
import torch.nn as nn
import torch.optim as opt
import matplotlib.pyplot as plt
import pymongo
import ssl
import random


LEARNING_RATE = 0.01
TOTAL_EPOCHS = 100
DB_URL = \
    "mongodb+srv://cs4701:password123!@cluster0-ao7be.mongodb.net/" + \
    "nbaData?retryWrites=true"


class NeuralNetwork(nn.Module):
    """A neural network with exactly 1 hidden layer."""
    def __init__(self, num_inputs, num_hidden, num_out):
        super(NeuralNetwork, self).__init__()

        self.layer1 = nn.Linear(num_inputs, num_hidden)
        self.layer2 = nn.Linear(num_hidden, num_out)

    def forward(self, X):
        h_out = torch.sigmoid(self.layer1(X))
        y_pred = torch.sigmoid(self.layer2(h_out))
        return y_pred


def train(X_train, y_train, model, loss_fn, optimizer):
    """Train with BCELoss as loss function (best for binary classification)
    and stochastic gradient descent as optimizer. Returns the loss for that
    iteration."""
    model.train()
    outputs = model(X_train)
    loss = loss_fn(outputs, y_train)

    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    return loss.item()


def validate(X_v, y_v, model, loss_fn, optimizer):
    """Computes the loss for our current model using the validation data."""
    model.eval()
    y_out = model(X_v)
    loss = loss_fn(y_out, y_v)
    return loss.item()


def accuracy(X_test, y_actual, model):
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


def get_team_stats():
    """Returns a list of all the team-level ML stats for each game, as well as
    the outcome. Each list has the form [0..., 0..., 0..., 0..., ..., 1/0]"""
    client = pymongo.MongoClient(DB_URL, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
    db = client.nbaData
    ml_stats = db.learningStats_v1
    parsed_stats = []
    for game_stats in ml_stats.find():
        del game_stats["_id"]
        del game_stats["game_id"]
        parsed_stats.append(list(game_stats.values()))
    return parsed_stats


def plot_stats(tl, vl, acc):
    """Uses matplotlib to show the training losses, validation losses,
    and validation accuracy over time."""
    iters = [i for i in range(TOTAL_EPOCHS)]
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


def partition_data(X_data, y_data):
    """Partitions the data into training, validation, and testing sets.
    Returns a tuple that contains X, y data for each."""
    num_train = int(0.8 * len(team_learning_data))
    num_validate = int(0.8 * (len(team_learning_data) - num_train))
    test_idx = num_train + num_validate

    X_train = torch.tensor(X_data[:num_train], dtype=torch.float)
    y_train = torch.tensor(y_data[:num_train], dtype=torch.float)
    X_v = torch.tensor(X_data[num_train:test_idx], dtype=torch.float)
    y_v = torch.tensor(y_data[num_train:test_idx], dtype=torch.float)
    X_test = torch.tensor(X_data[test_idx:], dtype=torch.float)
    y_test = torch.tensor(y_data[test_idx:], dtype=torch.float)

    return X_train, y_train, X_v, y_v, X_test, y_test


def gen_random_ml_data():
    """Generates random data of the same format as your team-stats ML data,
    for the sole purpose of comparing our neural network's performance to
    totally random data."""
    data = []
    for i in range(5000):
        temp = []
        for k in range(8):
            temp.append(random.uniform(0, 1))
        temp.append(random.choice([0, 1]))
        data.append(temp)
    return data


if __name__ == "__main__":
    team_learning_data = get_team_stats()
    random.shuffle(team_learning_data)

    X_data, y_data = split_xy(team_learning_data)
    X_train, y_train, X_v, y_v, X_test, y_test = partition_data(X_data, y_data)

    model = NeuralNetwork(8, 8, 1)
    loss_fn = nn.BCELoss()
    optimizer = opt.SGD(model.parameters(), lr=0.01, momentum=0.9)

    training_losses = []
    validation_losses = []
    accuracies = []

    for epoch in range(TOTAL_EPOCHS):
        tl = train(X_train, y_train, model, loss_fn, optimizer)
        vl = validate(X_v, y_v, model, loss_fn, optimizer)
        acc = accuracy(X_v, y_v, model)
        training_losses.append(tl)
        validation_losses.append(vl)
        accuracies.append(acc)
        log_str = "Epoch: {}/{}, Training Loss: {}, Validation Loss: {}, Accuracy: {}"\
            .format(epoch, TOTAL_EPOCHS, tl, vl, acc)
        print(log_str)

    plot_stats(training_losses, validation_losses, accuracies)
